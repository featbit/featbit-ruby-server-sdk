# frozen_string_literal: true

require "timeout"
require "websocket-client-simple"

module FeatBit
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
      rescue IOError, SystemCallError, OpenSSL::SSL::SSLError, Timeout::Error
        # A failed close handshake is harmless if the ensure cleanup succeeds.
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
