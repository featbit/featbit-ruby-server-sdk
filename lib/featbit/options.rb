# frozen_string_literal: true

require "logger"
require "uri"

module FeatBit
  class Options
    DEFAULT_STREAMING_URL = "wss://app-eval.featbit.co"
    DEFAULT_EVENT_URL = "https://app-eval.featbit.co"

    attr_reader :env_secret, :streaming_url, :event_url, :start_wait,
                :offline, :bootstrap, :disable_events, :logger,
                :events_capacity, :events_flush_interval, :events_batch_size,
                :connect_timeout, :read_timeout, :reconnect_delay,
                :data_store, :synchronizer_factory, :event_processor_factory

    def initialize(env_secret: "", streaming_url: DEFAULT_STREAMING_URL,
                   event_url: DEFAULT_EVENT_URL, start_wait: 5.0, offline: false,
                   bootstrap: nil, disable_events: false, logger: nil,
                   events_capacity: 10_000, events_flush_interval: 1.0,
                   events_batch_size: 50, connect_timeout: 5.0,
                   read_timeout: 10.0, reconnect_delay: 1.0,
                   data_store: nil, synchronizer_factory: nil,
                   event_processor_factory: nil)
      @env_secret = env_secret.to_s
      @streaming_url = normalize_url(streaming_url)
      @event_url = normalize_url(event_url)
      @start_wait = positive_number(start_wait, 5.0)
      @offline = offline == true
      @bootstrap = bootstrap
      @disable_events = disable_events == true
      @logger = logger || Logger.new($stderr, level: Logger::WARN)
      @events_capacity = positive_integer(events_capacity, 10_000)
      @events_flush_interval = positive_number(events_flush_interval, 1.0)
      @events_batch_size = positive_integer(events_batch_size, 50)
      @connect_timeout = positive_number(connect_timeout, 5.0)
      @read_timeout = positive_number(read_timeout, 10.0)
      @reconnect_delay = positive_number(reconnect_delay, 1.0)
      @data_store = data_store
      @synchronizer_factory = synchronizer_factory
      @event_processor_factory = event_processor_factory
      freeze
    rescue StandardError => e
      warn("FeatBit options error: #{e.message}")
      @env_secret = ""
      @streaming_url = DEFAULT_STREAMING_URL
      @event_url = DEFAULT_EVENT_URL
      @start_wait = 5.0
      @offline = true
      @bootstrap = nil
      @disable_events = true
      @logger = Logger.new($stderr, level: Logger::WARN)
      @events_capacity = 10_000
      @events_flush_interval = 1.0
      @events_batch_size = 50
      @connect_timeout = 5.0
      @read_timeout = 10.0
      @reconnect_delay = 1.0
      freeze
    end

    def streaming_uri
      append_path(streaming_url, "/streaming")
    end

    def events_uri
      append_path(event_url, "/api/public/insight/track")
    end

    def valid?
      return true if offline

      !env_secret.empty? && valid_uri?(streaming_uri, %w[ws wss]) && valid_uri?(events_uri, %w[http https])
    rescue StandardError
      false
    end

    private

    def normalize_url(value)
      value.to_s.sub(%r{/+\z}, "")
    end

    def append_path(base, path)
      base.end_with?(path) ? base : "#{base}#{path}"
    end

    def valid_uri?(value, schemes)
      uri = URI.parse(value)
      schemes.include?(uri.scheme) && !uri.host.to_s.empty?
    end

    def positive_number(value, fallback)
      number = Float(value)
      number.positive? ? number : fallback
    rescue StandardError
      fallback
    end

    def positive_integer(value, fallback)
      number = Integer(value)
      number.positive? ? number : fallback
    rescue StandardError
      fallback
    end
  end
end
