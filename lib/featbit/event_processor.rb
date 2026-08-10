# frozen_string_literal: true

require "json"
require "net/http"
require "timeout"
require "uri"

module FeatBit
  class EventProcessor
    class DeliveryRejected < StandardError; end

    FLUSH = "__flush__"
    STOP = "__stop__"
    CONTROL_TIMEOUT = 5.0

    def initialize(options, sender: nil)
      @options = options
      @sender = sender || method(:post_batch)
      @queue = SizedQueue.new(options.events_capacity)
      @control_queue = Queue.new
      @dropped_events = 0
      @drop_mutex = Mutex.new
      @state_mutex = Mutex.new
      @close_mutex = Mutex.new
      @closed = false
      @close_result = nil
      @thread = Thread.new { run }
      @thread.name = "featbit-event-processor" if @thread.respond_to?(:name=)
    rescue StandardError => e
      options.logger&.error("FeatBit event processor failed to start: #{e.message}")
      @closed = true
    end

    def enqueue(event)
      return false if event.nil?

      @state_mutex.synchronize do
        return false if @closed

        @queue.push(event, true)
        true
      end
    rescue ThreadError
      @drop_mutex.synchronize { @dropped_events += 1 }
      false
    rescue StandardError => e
      @options.logger&.warn("FeatBit event enqueue failed: #{e.message}")
      false
    end

    def flush
      return false if closed?

      ack = Queue.new
      @control_queue << { type: FLUSH, ack: ack }

      Timeout.timeout(CONTROL_TIMEOUT) { ack.pop } != false
    rescue StandardError => e
      @options.logger&.warn("FeatBit event flush failed: #{e.message}")
      false
    end

    def close
      @close_mutex.synchronize do
        return @close_result unless @close_result.nil?

        mark_closed
        return @close_result = true unless @thread&.alive?

        ack = Queue.new
        @control_queue << { type: STOP, ack: ack }
        delivered = Timeout.timeout(CONTROL_TIMEOUT) { ack.pop } != false
        @thread&.join(CONTROL_TIMEOUT)
        @close_result = delivered && !@thread&.alive?
      end
    rescue StandardError => e
      @options.logger&.warn("FeatBit event processor close failed: #{e.message}")
      false
    end

    def dropped_events
      @drop_mutex.synchronize { @dropped_events }
    rescue StandardError
      0
    end

    private

    def run
      batch = []
      deadline = monotonic_time + @options.events_flush_interval
      loop do
        event = pop_with_timeout([deadline - monotonic_time, 0].max)
        if control?(event, STOP)
          drain_events(batch)
          delivered = send_batch(batch)
          batch.clear
          event[:ack].push(delivered)
          break
        elsif control?(event, FLUSH)
          drain_events(batch)
          delivered = send_batch(batch)
          batch.clear
          event[:ack].push(delivered)
          deadline = monotonic_time + @options.events_flush_interval
        elsif event
          batch << event
          if batch.length >= @options.events_batch_size
            send_batch(batch)
            batch.clear
            deadline = monotonic_time + @options.events_flush_interval
          end
        elsif monotonic_time >= deadline
          send_batch(batch)
          batch.clear
          deadline = monotonic_time + @options.events_flush_interval
        end
      end
    rescue StandardError => e
      @options.logger&.error("FeatBit event processor stopped unexpectedly: #{e.message}")
    ensure
      mark_closed
    end

    def pop_with_timeout(timeout)
      deadline = monotonic_time + timeout
      loop do
        return @control_queue.pop(true)
      rescue ThreadError
        begin
          return @queue.pop(true)
        rescue ThreadError
          return nil if monotonic_time >= deadline

          sleep([deadline - monotonic_time, 0.01].min)
        end
      end
    end

    def drain_events(batch)
      loop do
        batch << @queue.pop(true)
      rescue ThreadError
        break
      end
      batch
    end

    def control?(event, type)
      event.is_a?(Hash) && event[:type] == type
    end

    def send_batch(batch)
      return true if batch.empty?

      delivered = true
      batch.each_slice(@options.events_batch_size) do |slice|
        delivered = false unless send_slice(slice)
      end
      delivered
    end

    def send_slice(batch)
      attempts = 0

      begin
        attempts += 1
        delivered = @sender.call(batch.dup)
        raise "sender rejected batch" if delivered == false

        true
      rescue DeliveryRejected
        raise
      rescue StandardError
        retry if attempts < 3

        raise
      end
    rescue StandardError => e
      @options.logger&.warn("FeatBit event delivery failed: #{e.message}")
      false
    end

    def post_batch(batch)
      uri = URI.parse(@options.events_uri)
      request = Net::HTTP::Post.new(uri)
      request["Authorization"] = @options.env_secret
      request["User-Agent"] = "featbit-ruby-server-sdk/#{FeatBit::VERSION}"
      request["Content-Type"] = "application/json"
      request.body = JSON.generate(batch)
      response = Net::HTTP.start(
        uri.host, uri.port,
        use_ssl: uri.scheme == "https",
        open_timeout: @options.connect_timeout,
        read_timeout: @options.read_timeout
      ) { |http| http.request(request) }
      unless response.is_a?(Net::HTTPSuccess)
        message = response.body.to_s.gsub(/\s+/, " ").strip[0, 200]
        raise DeliveryRejected, ["HTTP #{response.code}", message].reject(&:empty?).join(": ")
      end

      true
    end

    def closed?
      @state_mutex.synchronize { @closed }
    rescue StandardError
      true
    end

    def mark_closed
      @state_mutex.synchronize { @closed = true }
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  class NullEventProcessor
    def enqueue(_event) = false
    def flush = true
    def close = true
    def dropped_events = 0
  end
end
