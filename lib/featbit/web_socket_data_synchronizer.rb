# frozen_string_literal: true

require "json"
require "securerandom"
require "time"
require "timeout"
require "websocket-client-simple"

module FeatBit
  class WebSocketDataSynchronizer
    ALPHABETS = { "0" => "Q", "1" => "B", "2" => "W", "3" => "S", "4" => "P",
                  "5" => "H", "6" => "D", "7" => "X", "8" => "Z", "9" => "U" }.freeze
    PING_INTERVAL = 10.0

    def initialize(options:, data_store:, status_provider:, on_flags_changed: nil, connector: nil)
      @options = options
      @data_store = data_store
      @status_provider = status_provider
      @on_flags_changed = on_flags_changed
      @connector = connector || method(:connect)
      @closed = false
      @socket_mutex = Mutex.new
      @lifecycle_mutex = Mutex.new
      @thread = nil
      @close_result = nil
    end

    def start
      @lifecycle_mutex.synchronize do
        return false if @closed || @thread&.alive?

        @thread = Thread.new { run }
        @thread.name = "featbit-websocket-sync" if @thread.respond_to?(:name=)
        true
      end
    rescue StandardError => e
      fail_status(e)
      false
    end

    def close
      @lifecycle_mutex.synchronize do
        return @close_result unless @close_result.nil?

        @closed = true
        socket = @socket_mutex.synchronize { @socket }
        socket_closed = safe_close_socket(socket)
        @thread&.join(5) unless Thread.current.equal?(@thread)
        @close_result = socket_closed && !@thread&.alive?
      end
    rescue StandardError => e
      @options.logger&.warn("FeatBit synchronizer close failed: #{e.message}")
      false
    end

    def process_message(message)
      envelope = message.is_a?(String) ? JSON.parse(message) : message
      return false unless fetch(envelope, "messageType") == "data-sync"

      data = fetch(envelope, "data", {})
      event_type = fetch(data, "eventType")
      return false unless valid_data?(data, event_type)

      old_keys = @data_store.all_flags.keys
      if event_type == "full"
        changed = @data_store.init(data)
        changed_keys = changed ? old_keys | @data_store.all_flags.keys : []
      else
        changed, changed_keys = process_patch(data)
      end

      changed_keys.each { |key| safely_notify(key) }
      return false unless changed

      @status_provider.update(Status::READY)
      true
    rescue JSON::ParserError => e
      @status_provider.update(Status::FAILED, message: "invalid data: #{e.message}")
      false
    rescue StandardError => e
      fail_status(e)
      false
    end

    private

    def run
      delay = @options.reconnect_delay
      until @closed
        socket = nil
        begin
          socket = @connector.call(websocket_url, headers)
          @socket_mutex.synchronize { @socket = socket }
          break if @closed

          configure_socket(socket)
          delay = @options.reconnect_delay
          next_ping = monotonic_time + PING_INTERVAL
          until @closed || socket_closed?(socket)
            if monotonic_time >= next_ping
              socket.send(JSON.generate(messageType: "ping", data: nil))
              next_ping = monotonic_time + PING_INTERVAL
            end
            interruptible_sleep(0.05)
          end
          break if @closed

          @status_provider.update(Status::INTERRUPTED, message: "WebSocket disconnected")
          interruptible_sleep(delay)
          delay = [delay * 2, 30.0].min
        rescue StandardError => e
          fail_status(e, interrupted: true)
          safe_close_socket(socket)
          interruptible_sleep(delay) unless @closed
          delay = [delay * 2, 30.0].min
        ensure
          safe_close_socket(socket) if @closed
          @socket_mutex.synchronize { @socket = nil }
        end
      end
    end

    def connect(url, request_headers)
      Timeout.timeout(@options.connect_timeout) do
        WebSocket::Client::Simple.connect(url, headers: request_headers)
      end
    end

    def configure_socket(socket)
      open_handler = method(:handle_socket_open)
      message_handler = method(:handle_socket_message)
      error_handler = method(:handle_socket_error)
      close_handler = method(:handle_socket_close)

      socket.on(:open) { open_handler.call(socket) }
      socket.on(:message) { |event| message_handler.call(socket, event) }
      socket.on(:error) { |event| error_handler.call(socket, event) }
      socket.on(:close) { |event| close_handler.call(event) }
    end

    def handle_socket_open(socket)
      socket.send(JSON.generate(messageType: "data-sync", data: { timestamp: @data_store.version }))
    rescue StandardError => e
      fail_status(e)
    end

    def handle_socket_message(socket, event)
      return if process_message(event.respond_to?(:data) ? event.data : event.to_s)

      safe_close_socket(socket) unless @closed
    end

    def handle_socket_error(socket, event)
      return if @closed

      fail_status(event.respond_to?(:message) ? event.message : event, interrupted: true)
      safe_close_socket(socket)
    end

    def handle_socket_close(_event)
      @status_provider.update(Status::INTERRUPTED, message: "WebSocket closed") unless @closed
    end

    def socket_closed?(socket)
      return socket.closed? if socket.respond_to?(:closed?)
      return !socket.open? if socket.respond_to?(:open?)

      false
    rescue StandardError
      true
    end

    def safe_close_socket(socket)
      return true unless socket

      socket.close != false
    rescue StandardError => e
      @options.logger&.warn("FeatBit WebSocket close failed: #{e.message}")
      false
    end

    def interruptible_sleep(duration)
      deadline = monotonic_time + duration.to_f
      until @closed
        remaining = deadline - monotonic_time
        break unless remaining.positive?

        sleep([remaining, 0.05].min)
      end
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def process_patch(data)
      items = Array(fetch(data, "featureFlags", [])).map { |flag| [:flags, flag] }
      items.concat(Array(fetch(data, "segments", [])).map { |segment| [:segments, segment] })
      changed_keys = []
      items.sort_by { |_kind, item| item_version(item) }.each do |kind, item|
        applied = @data_store.upsert(kind, item, version: item_version(item))
        if applied && kind == :flags
          changed_keys << fetch(item, "key").to_s
        elsif applied
          changed_keys.concat(flags_referencing_segment(fetch(item, "id").to_s))
        end
      end
      [true, changed_keys.uniq]
    end

    def valid_data?(data, event_type)
      data.is_a?(Hash) &&
        %w[full patch].include?(event_type) &&
        fetch(data, "featureFlags").is_a?(Array) &&
        fetch(data, "segments").is_a?(Array)
    end

    def flags_referencing_segment(segment_id)
      @data_store.all_flags.each_with_object([]) do |(flag_key, flag), result|
        referenced = Array(fetch(flag, "rules", [])).any? do |rule|
          Array(fetch(rule, "conditions", [])).any? do |condition|
            next false unless fetch(condition, "op").nil?

            serialized = fetch(condition, "value", "[]")
            ids = serialized.is_a?(String) ? JSON.parse(serialized) : serialized
            Array(ids).map(&:to_s).include?(segment_id)
          rescue JSON::ParserError
            false
          end
        end
        result << flag_key if referenced
      end
    end

    def item_version(item)
      explicit = fetch(item, "timestamp")
      return explicit.to_i if explicit

      updated = fetch(item, "updatedAt")
      updated ? (Time.parse(updated.to_s).to_f * 1000).to_i : @data_store.version + 1
    rescue StandardError
      @data_store.version + 1
    end

    def websocket_url
      "#{@options.streaming_uri}?token=#{build_token(@options.env_secret)}&type=server"
    end

    def headers
      {
        "Authorization" => @options.env_secret,
        "User-Agent" => "featbit-ruby-server-sdk/#{FeatBit::VERSION}",
        "Content-Type" => "application/json"
      }
    end

    def build_token(secret)
      text = secret.to_s.delete_suffix("=")
      timestamp = (Time.now.to_f * 1000).round.to_s
      timestamp_code = encode_number(timestamp, timestamp.length)
      start = [SecureRandom.random_number([text.length, 1].max), 2].max
      start = text.length if start > text.length
      "#{encode_number(start, 3)}#{encode_number(timestamp_code.length, 2)}#{text[0, start]}#{timestamp_code}#{text[start..]}"
    end

    def encode_number(number, length)
      padded = number.to_s.rjust([12, length].max, "0")
      padded[-length, length].chars.map { |character| ALPHABETS.fetch(character) }.join
    end

    def safely_notify(flag_key)
      @on_flags_changed&.call(flag_key)
    rescue StandardError => e
      @options.logger&.warn("FeatBit flag listener failed: #{e.message}")
    end

    def fail_status(error, interrupted: false)
      message = error.respond_to?(:message) ? error.message : error.to_s
      @options.logger&.warn("FeatBit WebSocket synchronization failed: #{message}")
      @status_provider.update(interrupted ? Status::INTERRUPTED : Status::FAILED, message: message)
    end

    def fetch(hash, key, default = nil)
      hash.is_a?(Hash) ? hash.fetch(key.to_s, hash.fetch(key.to_sym, default)) : default
    end
  end
end
