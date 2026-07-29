# frozen_string_literal: true

class FeatBitClientRegistry
  def initialize(env:, logger:, client_factory: nil)
    @env = env
    @logger = logger
    @client_factory = client_factory || ->(options) { FeatBit::Client.new(options) }
    @mutex = Mutex.new
    @client = nil
    @closed = false
    @close_result = nil
  end

  def client
    @mutex.synchronize do
      raise "FeatBit client registry is closed" if @closed

      @client ||= @client_factory.call(build_options)
    end
  end

  def close
    @mutex.synchronize do
      return @close_result unless @close_result.nil?

      @closed = true
      @close_result = @client.nil? || @client.close != false
    end
  rescue StandardError => e
    @logger&.warn("FeatBit Rails shutdown failed: #{e.message}")
    @mutex.synchronize { @close_result = false }
  end

  private

  def build_options
    FeatBit::Options.new(
      env_secret: @env.fetch("FEATBIT_ENV_SECRET"),
      streaming_url: @env.fetch("FEATBIT_STREAMING_URL", "wss://app-eval.featbit.co"),
      event_url: @env.fetch("FEATBIT_EVENT_URL", "https://app-eval.featbit.co"),
      start_wait: 10,
      logger: @logger
    )
  end
end
