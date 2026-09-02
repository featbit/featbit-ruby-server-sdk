# frozen_string_literal: true

require_relative "../status"

module FeatBit
  class WebSocketClosePolicy
    SERVER_REJECTED_CODE = 4003

    def initialize(status_provider)
      @status_provider = status_provider
      @mutex = Mutex.new
      @rejected = false
    end

    def close_frame?(event)
      event.respond_to?(:type) && event.type.to_sym == :close
    rescue StandardError
      false
    end

    def reject?(event)
      return false unless close_code(event) == SERVER_REJECTED_CODE

      @mutex.synchronize { @rejected = true }
      reason = close_reason(event)
      message = "WebSocket connection rejected by server (4003)"
      message = "#{message}: #{reason}" unless reason.empty?
      @status_provider.update(Status::FAILED, message: message)
      true
    end

    def rejected?
      @mutex.synchronize { @rejected }
    end

    private

    def close_code(event)
      return event.code.to_i if event.respond_to?(:code) && event.code
      return event[:code].to_i if event.is_a?(Hash) && event[:code]

      event["code"].to_i if event.is_a?(Hash) && event["code"]
    end

    def close_reason(event)
      value = if event.respond_to?(:reason) && event.reason
                event.reason
              elsif event.respond_to?(:data) && event.data
                event.data
              end
      value.to_s
    end
  end
end
