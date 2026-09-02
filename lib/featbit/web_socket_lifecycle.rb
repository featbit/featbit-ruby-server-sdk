# frozen_string_literal: true

require "timeout"
require "websocket-client-simple"

module FeatBit
  # Owns shutdown and drains callbacks without holding locks around user code.
  class WebSocketLifecycle
    CLOSE_WAIT = 5.0

    def initialize
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @callbacks = Hash.new(0)
      @stopped = false
      @clean = true
      @attempt = nil
    end

    def start(&block)
      @mutex.synchronize do
        return false if @stopped || @thread&.alive?

        @thread = Thread.new(&block)
        @thread.name = "featbit-websocket-sync" if @thread.respond_to?(:name=)
        true
      end
    end

    def stopped?
      @mutex.synchronize { @stopped }
    end

    def close
      thread, in_callback = @mutex.synchronize do
        @stopped = true
        [@thread, @callbacks.key?(Thread.current)]
      end
      return false if in_callback || Thread.current.equal?(thread)

      thread&.join(CLOSE_WAIT)
      @mutex.synchronize { !thread&.alive? && @clean }
    end

    def activate(attempt)
      @mutex.synchronize { @attempt = attempt }
    end

    def dispatch(attempt)
      entered = @mutex.synchronize do
        unless @stopped || !@attempt.equal?(attempt)
          @callbacks[Thread.current] += 1
          true
        end
      end
      yield if entered
    ensure
      if entered
        @mutex.synchronize do
          @callbacks[Thread.current] -= 1
          @callbacks.delete(Thread.current) if @callbacks[Thread.current].zero?
          @condition.broadcast
        end
      end
    end

    def finish(attempt)
      @mutex.synchronize do
        @attempt = nil
        @condition.wait(@mutex) until @callbacks.empty?
      end
      clean = attempt.cleanup
      @mutex.synchronize do
        @clean &&= clean
        @stopped = true unless clean
      end
    end
  end

  # websocket-client-simple has no abort API. Keep its internal cleanup here,
  # after the connector has exited and SDK callbacks have drained.
  class ClosableWebSocketClient < WebSocket::Client::Simple::Client
    CLOSE_TIMEOUT = 1.0

    def close(drain: false)
      # The library also calls close from its own I/O error callbacks. Only the
      # SDK owner may drain/kill the reader, after application callbacks return.
      unless drain
        @closed = true
        return true
      end

      begin
        Timeout.timeout(CLOSE_TIMEOUT) { super() }
      rescue Timeout::Error
        # A stalled close-frame write must not prevent closing the TCP socket.
      ensure
        @closed = true
        begin
          @socket&.close
          @socket = nil
        ensure
          @thread&.kill
          @thread&.join unless Thread.current.equal?(@thread)
        end
      end
      true
    end
  end
end
