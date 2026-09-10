# frozen_string_literal: true

require "timeout"

module FeatBit
  class WebSocketConnectionAttempt
    attr_reader :socket

    def initialize(timeout:, clock:, lifecycle:, closer:)
      @deadline = clock.call + timeout
      @clock = clock
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @result = nil
      @lifecycle = lifecycle
      @closer = closer
      @finished = false
    end

    def connect(connector, url, headers)
      @connector_thread = Thread.new do
        configured = false
        @socket = connector.call(url, headers) do |connected_socket|
          @socket = connected_socket
          yield connected_socket
          configured = true
        end
        yield @socket unless configured
      rescue StandardError => e
        @connect_error = e
      end
      until @connector_thread.join(0.01)
        break if @lifecycle.stopped? || @clock.call >= @deadline
      end
      return @socket if @lifecycle.stopped?

      raise Timeout::Error, "WebSocket connection timed out" if @connector_thread.alive?
      raise @connect_error if @connect_error

      @socket
    end

    def cleanup
      # Cancellation is confined to the owned connector, never an application
      # callback. Joining before close prevents a late connector creating a socket.
      @connector_thread&.kill if @connector_thread&.alive?
      @connector_thread&.join
      @closer.call(@socket)
    end

    def signal(result)
      @mutex.synchronize do
        @finished = true unless result == :opened
        return if @result

        @result = result
        @condition.broadcast
      end
    end

    def wait(stopped:)
      @mutex.synchronize do
        until @result || stopped.call
          remaining = @deadline - @clock.call
          return :timeout unless remaining.positive?

          @condition.wait(@mutex, [remaining, 0.05].min)
        end
        stopped.call ? :stopped : @result
      end
    end

    def monitor(stopped:, ping:, interval:)
      next_ping = @clock.call + interval
      until stopped.call || @mutex.synchronize { @finished } || socket_closed?
        if @clock.call >= next_ping
          ping.call(@socket)
          next_ping = @clock.call + interval
        end
        sleep(0.05)
      end
    end

    private

    def socket_closed?
      return @socket.closed? if @socket.respond_to?(:closed?)
      return !@socket.open? if @socket.respond_to?(:open?)

      false
    rescue StandardError
      true
    end
  end
end
