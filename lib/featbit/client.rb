# frozen_string_literal: true

require "json"

module FeatBit
  class Client
    attr_reader :options, :status_provider, :event_processor, :data_store

    def initialize(options = Options.new)
      @options = options.is_a?(Options) ? options : Options.new(offline: true)
      @data_store = @options.data_store || InMemoryDataStore.new
      initial_status = @options.offline ? Status::OFFLINE : Status::STARTING
      @status_provider = StatusProvider.new(initial_status, logger: @options.logger)
      @listeners = {}
      @listener_id = 0
      @listener_mutex = Mutex.new
      @lifecycle_mutex = Mutex.new
      @closed = false
      @close_result = nil

      load_bootstrap(@options.bootstrap) if @options.bootstrap
      @event_processor = build_event_processor
      @evaluator = Evaluator.new(
        flag_getter: ->(key) { @data_store.flag(key) },
        segment_getter: ->(id) { @data_store.segment(id) },
        logger: @options.logger
      )
      start_synchronizer unless @options.offline
      @status_provider.update(Status::READY) if @options.offline && @data_store.initialized?
      @status_provider.wait_until_ready(@options.start_wait) if !@options.offline && @options.start_wait.positive?
    rescue StandardError => e
      safe_log(:error, "FeatBit client initialization failed: #{e.message}")
      @status_provider ||= StatusProvider.new(Status::FAILED)
      @status_provider.update(Status::FAILED, message: e.message)
      @event_processor ||= NullEventProcessor.new
    end

    def initialized?
      @status_provider.ready?
    rescue StandardError
      false
    end

    def variation(flag_key, user, default_value)
      variation_detail(flag_key, user, default_value).value
    rescue StandardError => e
      safe_log(:error, "FeatBit variation failed: #{e.message}")
      default_value
    end

    def variation_detail(flag_key, user, default_value)
      return safe_detail(flag_key, default_value, :client_not_ready) if @closed || !evaluable?

      normalized_user = normalize_user(user)
      detail = @evaluator.evaluate(flag_key, normalized_user, default_value)
      enqueue_evaluation(detail, normalized_user) if detail.success?
      detail
    rescue StandardError => e
      safe_log(:error, "FeatBit variation detail failed: #{e.message}")
      safe_detail(flag_key, default_value, :error, e.message)
    end

    def bool_variation(flag_key, user, default_value = false)
      typed_variation(flag_key, user, default_value, [TrueClass, FalseClass])
    end

    def string_variation(flag_key, user, default_value = "")
      typed_variation(flag_key, user, default_value, [String])
    end

    def number_variation(flag_key, user, default_value = 0)
      typed_variation(flag_key, user, default_value, [Numeric])
    end

    def json_variation(flag_key, user, default_value = {})
      typed_variation(flag_key, user, default_value, [Hash, Array])
    end

    def track(user, event_name, numeric_value = 1.0, properties: {})
      normalized_user = normalize_user(user)
      return false unless normalized_user.valid? && !event_name.to_s.empty?

      @event_processor.enqueue(
        user: normalized_user.to_h,
        metrics: [{
          eventName: event_name.to_s,
          numericValue: numeric_value.to_f,
          route: "index/metric",
          type: "CustomEvent",
          appType: "rubyserverside",
          timestamp: timestamp_ms,
          properties: properties || {}
        }]
      )
    rescue StandardError => e
      safe_log(:warn, "FeatBit track failed: #{e.message}")
      false
    end

    def flush
      @event_processor.flush
    rescue StandardError
      false
    end

    def add_flag_change_listener(callable = nil, &block)
      listener = callable || block
      return nil unless listener.respond_to?(:call)

      @listener_mutex.synchronize do
        @listener_id += 1
        @listeners[@listener_id] = listener
        @listener_id
      end
    rescue StandardError
      nil
    end

    def remove_flag_change_listener(id)
      @listener_mutex.synchronize { !@listeners.delete(id).nil? }
    rescue StandardError
      false
    end

    def close
      @lifecycle_mutex.synchronize do
        return @close_result unless @close_result.nil?

        @closed = true
        results = [safe_close(@synchronizer), safe_close(@event_processor)]
        results << safe_status_update(Status::CLOSED)
        @close_result = results.all?
      end
    rescue StandardError => e
      safe_log(:warn, "FeatBit client close failed: #{e.message}")
      false
    end

    alias stop close

    private

    def evaluable?
      @data_store.initialized? && [Status::READY, Status::OFFLINE, Status::INTERRUPTED].include?(@status_provider.status)
    end

    def load_bootstrap(bootstrap)
      success = @data_store.init(bootstrap)
      @status_provider.update(success ? Status::READY : Status::FAILED, message: success ? nil : "invalid bootstrap")
    rescue StandardError => e
      @status_provider.update(Status::FAILED, message: e.message)
    end

    def build_event_processor
      return NullEventProcessor.new if @options.offline || @options.disable_events
      return @options.event_processor_factory.call(@options) if @options.event_processor_factory

      EventProcessor.new(@options)
    rescue StandardError => e
      safe_log(:warn, "FeatBit event processor unavailable: #{e.message}")
      NullEventProcessor.new
    end

    def start_synchronizer
      unless @options.valid?
        @status_provider.update(Status::FAILED, message: "invalid options")
        return
      end

      @synchronizer = if @options.synchronizer_factory
                        @options.synchronizer_factory.call(@options, @data_store, @status_provider, method(:broadcast_flag_change))
                      else
                        WebSocketDataSynchronizer.new(
                          options: @options,
                          data_store: @data_store,
                          status_provider: @status_provider,
                          on_flags_changed: method(:broadcast_flag_change)
                        )
                      end
      @synchronizer.start
    rescue StandardError => e
      @status_provider.update(Status::FAILED, message: e.message)
    end

    def typed_variation(flag_key, user, default_value, expected_types)
      value = variation(flag_key, user, default_value)
      expected_types.any? { |type| value.is_a?(type) } ? value : default_value
    rescue StandardError
      default_value
    end

    def normalize_user(user)
      return user if user.is_a?(User)
      return User.new(user) if user.is_a?(String)

      if user.is_a?(Hash)
        key = user["key"] || user[:key] || user["keyId"] || user[:keyId] || user["targeting_key"] || user[:targeting_key]
        name = user["name"] || user[:name]
        custom = user.reject { |candidate, _| %w[key keyId targeting_key name].include?(candidate.to_s) }
        return User.new(key, name: name, custom: custom)
      end

      User.new("")
    rescue StandardError
      User.new("")
    end

    def enqueue_evaluation(detail, user)
      @event_processor.enqueue(
        user: user.to_h,
        variations: [{
          featureFlagKey: detail.flag_key,
          sendToExperiment: detail.send_to_experiment == true,
          timestamp: timestamp_ms,
          variation: { id: detail.variation_id, value: serialize_variation_value(detail.value), reason: detail.reason }
        }]
      )
    end

    def serialize_variation_value(value)
      case value
      when String then value
      when TrueClass then "true"
      when FalseClass then "false"
      when Hash, Array then JSON.generate(value)
      when nil then ""
      else value.to_s
      end
    rescue StandardError
      ""
    end

    def broadcast_flag_change(flag_key)
      listeners = @listener_mutex.synchronize { @listeners.values.dup }
      listeners.each do |listener|
        listener.call(flag_key)
      rescue StandardError => e
        safe_log(:warn, "FeatBit flag listener failed: #{e.message}")
      end
      true
    rescue StandardError
      false
    end

    def safe_detail(flag_key, default_value, reason, message = nil)
      EvaluationDetail.new(
        value: default_value,
        reason: Evaluator::REASONS.fetch(reason),
        flag_key: flag_key,
        error_kind: reason,
        error_message: message
      )
    end

    def safe_log(level, message)
      @options&.logger&.public_send(level, message)
    rescue StandardError
      nil
    end

    def safe_close(component)
      return true unless component

      component.close != false
    rescue StandardError => e
      safe_log(:warn, "FeatBit component close failed: #{e.message}")
      false
    end

    def safe_status_update(status)
      @status_provider.update(status) != false
    rescue StandardError => e
      safe_log(:warn, "FeatBit status update failed: #{e.message}")
      false
    end

    def timestamp_ms
      (Time.now.to_f * 1000).round
    end
  end
end
