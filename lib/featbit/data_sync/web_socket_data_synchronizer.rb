# frozen_string_literal: true

require "json"
require "securerandom"
require "time"
require "timeout"
require_relative "synchronization_result"
require_relative "web_socket_close_policy"
require_relative "web_socket_connection_attempt"
require_relative "web_socket_lifecycle"
require_relative "closable_web_socket_client"

module FeatBit
  class WebSocketDataSynchronizer
    ALPHABETS = { "0" => "Q", "1" => "B", "2" => "W", "3" => "S", "4" => "P",
                  "5" => "H", "6" => "D", "7" => "X", "8" => "Z", "9" => "U" }.freeze
    PING_INTERVAL = 10.0

    def initialize(options:, data_store:, status_provider:, on_flags_changed: nil, connector: nil)
      @options = options
      @data_store = data_store
      @status_provider = status_provider
      @close_policy = WebSocketClosePolicy.new(status_provider)
      @on_flags_changed = on_flags_changed
      @connector = connector || method(:connect)
      @lifecycle = WebSocketLifecycle.new
    end

    def start
      @lifecycle.start { run }
    rescue StandardError => e
      fail_status(e)
      false
    end

    def close
      @lifecycle.close
    rescue StandardError => e
      @options.logger&.warn("FeatBit synchronizer close failed: #{e.message}")
      false
    end

    def process_message(message)
      return SynchronizationResult::INVALID if @lifecycle.stopped?

      envelope = message.is_a?(String) ? JSON.parse(message) : message
      return SynchronizationResult::INVALID unless fetch(envelope, "messageType") == "data-sync"

      data = fetch(envelope, "data", {})
      event_type = fetch(data, "eventType")
      return SynchronizationResult::INVALID unless valid_data?(data, event_type)

      old_keys = @data_store.all_flags.keys
      if event_type == "full"
        changed = @data_store.init(data)
        changed_keys = changed ? old_keys | @data_store.all_flags.keys : []
      else
        changed, changed_keys = process_patch(data)
      end

      changed_keys.each { |key| safely_notify(key) unless @lifecycle.stopped? }
      @status_provider.update(Status::READY) unless @lifecycle.stopped?
      SynchronizationResult.valid(changed: changed)
    rescue JSON::ParserError => e
      @status_provider.update(Status::FAILED, message: "invalid data: #{e.message}")
      SynchronizationResult::INVALID
    rescue StandardError => e
      fail_status(e)
      SynchronizationResult::INVALID
    end

    private

    def run
      delay = @options.reconnect_delay
      until @lifecycle.stopped? || @close_policy.rejected?
        opened = run_attempt
        break if @lifecycle.stopped? || @close_policy.rejected?

        delay = @options.reconnect_delay if opened
        interruptible_sleep(delay)
        delay = [delay * 2, 30.0].min
      end
    end

    def run_attempt
      attempt = WebSocketConnectionAttempt.new(
        timeout: @options.connect_timeout, clock: method(:monotonic_time), lifecycle: @lifecycle, closer: method(:safe_close_socket)
      )
      @lifecycle.activate(attempt)
      attempt.connect(@connector, websocket_url, headers) { |socket| configure_socket(socket, attempt) }
      result = attempt.wait(stopped: @lifecycle.method(:stopped?))
      raise Timeout::Error, "WebSocket handshake timed out" if result == :timeout
      return false unless result == :opened

      attempt.monitor(stopped: @lifecycle.method(:stopped?), ping: method(:send_ping), interval: PING_INTERVAL)
      handle_socket_close(nil) unless @lifecycle.stopped? || @close_policy.rejected?
      true
    rescue StandardError => e
      fail_status(e, interrupted: true) unless @close_policy.rejected?
      false
    ensure
      @lifecycle.finish(attempt) if attempt
    end

    def send_ping(socket) = socket.send(JSON.generate(messageType: "ping", data: nil))

    def connect(url, request_headers, &configure)
      socket = ClosableWebSocketClient.new
      configure.call(socket)
      socket.connect(url, headers: request_headers)
      socket
    end

    def configure_socket(socket, attempt = nil)
      lifecycle = @lifecycle
      handlers = {
        open: ->(_) { handle_socket_open(socket, attempt) },
        message: ->(event) { handle_socket_message(socket, event, attempt) },
        error: ->(event) { handle_socket_error(socket, event, attempt) },
        close: ->(event) { handle_socket_close(event, attempt) }
      }
      handlers.each do |name, handler|
        socket.on(name) { |event| lifecycle.dispatch(attempt) { handler.call(event) } }
      end
    end

    def handle_socket_open(socket, attempt = nil)
      socket.send(JSON.generate(messageType: "data-sync", data: { timestamp: @data_store.version }))
      attempt&.signal(:opened)
    rescue StandardError => e
      fail_status(e)
      attempt&.signal(:failed)
      safe_close_socket(socket) unless attempt
    end

    def handle_socket_message(socket, event, attempt = nil)
      if @close_policy.close_frame?(event)
        handle_socket_close(event, attempt)
        safe_close_socket(socket) unless attempt
        return
      end

      result = process_message(event.respond_to?(:data) ? event.data : event.to_s)
      return if result.valid?

      attempt&.signal(:failed)
      safe_close_socket(socket) unless attempt || @lifecycle.stopped?
    end

    def handle_socket_error(socket, event, attempt = nil)
      return if @lifecycle.stopped? || @close_policy.rejected?

      error = event.respond_to?(:message) ? event.message : event
      fail_status(error, interrupted: true)
      attempt&.signal(:failed)
      safe_close_socket(socket) unless attempt
    end

    def handle_socket_close(event, attempt = nil)
      return if @lifecycle.stopped?

      rejected = @close_policy.reject?(event)
      attempt&.signal(rejected ? :rejected : :closed)
      return if rejected || @close_policy.rejected?

      @status_provider.update(Status::INTERRUPTED, message: "WebSocket closed")
    end

    def safe_close_socket(socket)
      return true unless socket

      Timeout.timeout(2) { (socket.is_a?(ClosableWebSocketClient) ? socket.close(drain: true) : socket.close) != false }
    rescue StandardError => e
      @options.logger&.warn("FeatBit WebSocket close failed: #{e.message}")
      false
    end

    def interruptible_sleep(duration)
      deadline = monotonic_time + duration.to_f
      until @lifecycle.stopped?
        remaining = deadline - monotonic_time
        break unless remaining.positive?

        sleep([remaining, 0.05].min)
      end
    end

    def monotonic_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def process_patch(data)
      items = Array(fetch(data, "featureFlags", [])).map { |flag| [:flags, flag] }
      items.concat(Array(fetch(data, "segments", [])).map { |segment| [:segments, segment] })
      changed = false
      changed_keys = []
      items.sort_by { |_kind, item| item_version(item) }.each do |kind, item|
        applied = @data_store.upsert(kind, item, version: item_version(item))
        changed = true if applied
        if applied && kind == :flags
          changed_keys << fetch(item, "key").to_s
        elsif applied
          changed_keys.concat(flags_referencing_segment(fetch(item, "id").to_s))
        end
      end
      [changed, changed_keys.uniq]
    end

    def valid_data?(data, event_type)
      flags = fetch(data, "featureFlags")
      segments = fetch(data, "segments")
      data.is_a?(Hash) &&
        %w[full patch].include?(event_type) &&
        flags.is_a?(Array) && flags.all? { |flag| !fetch(flag, "key").to_s.empty? } &&
        segments.is_a?(Array) && segments.all? { |segment| !fetch(segment, "id").to_s.empty? }
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

    def websocket_url = "#{@options.streaming_uri}?token=#{build_token(@options.env_secret)}&type=server"

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
      return if @lifecycle.stopped?

      message = error.respond_to?(:message) ? error.message : error.to_s
      @options.logger&.warn("FeatBit WebSocket synchronization failed: #{message}")
      @status_provider.update(interrupted ? Status::INTERRUPTED : Status::FAILED, message: message)
    end

    def fetch(hash, key, default = nil)
      hash.is_a?(Hash) ? hash.fetch(key.to_s, hash.fetch(key.to_sym, default)) : default
    end
  end
end
