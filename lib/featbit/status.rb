# frozen_string_literal: true

module FeatBit
  module Status
    STARTING = :starting
    READY = :ready
    INTERRUPTED = :interrupted
    OFFLINE = :offline
    FAILED = :failed
    CLOSED = :closed
  end

  class StatusProvider
    def initialize(initial = Status::STARTING, logger: nil)
      @status = initial
      @message = nil
      @listeners = {}
      @next_listener_id = 0
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @logger = logger
    end

    def status
      @mutex.synchronize { @status }
    end

    def message
      @mutex.synchronize { @message }
    end

    def ready?
      status == Status::READY
    end

    def wait_until_ready(timeout = 5.0)
      deadline = monotonic_time + timeout.to_f
      @mutex.synchronize do
        until @status == Status::READY
          return false if [Status::FAILED, Status::CLOSED].include?(@status)

          remaining = deadline - monotonic_time
          return false unless remaining.positive?

          @condition.wait(@mutex, remaining)
        end
      end
      true
    rescue StandardError
      false
    end

    def add_listener(callable = nil, &block)
      listener = callable || block
      return nil unless listener.respond_to?(:call)

      @mutex.synchronize do
        @next_listener_id += 1
        @listeners[@next_listener_id] = listener
        @next_listener_id
      end
    rescue StandardError
      nil
    end

    def remove_listener(id)
      @mutex.synchronize { !@listeners.delete(id).nil? }
    rescue StandardError
      false
    end

    def update(new_status, message: nil)
      listeners = @mutex.synchronize do
        changed = @status != new_status || @message != message
        @status = new_status
        @message = message
        @condition.broadcast
        changed ? @listeners.values.dup : []
      end
      listeners.each do |listener|
        listener.call(new_status, message)
      rescue StandardError => e
        safe_log(:warn, "FeatBit status listener failed: #{e.message}")
      end
      true
    rescue StandardError => e
      safe_log(:warn, "FeatBit status update failed: #{e.message}")
      false
    end

    private

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def safe_log(level, message)
      @logger&.public_send(level, message)
    rescue StandardError
      nil
    end
  end
end
