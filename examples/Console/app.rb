# frozen_string_literal: true

require "featbit"

options = FeatBit::Options.new(
  env_secret: ENV.fetch("FEATBIT_ENV_SECRET"),
  streaming_url: ENV.fetch("FEATBIT_STREAMING_URL", "wss://app-eval.featbit.co"),
  event_url: ENV.fetch("FEATBIT_EVENT_URL", "https://app-eval.featbit.co"),
  start_wait: 10
)

client = FeatBit::Client.new(options)
flag_key = ENV.fetch("FEATBIT_FLAG_KEY", "game-runner")
user = FeatBit::User.new(
  ENV.fetch("FEATBIT_USER_KEY", "console-user"),
  name: "Ruby Console Example",
  custom: { application: "console", language: "ruby" }
)

begin
  warn "FeatBit is not ready; the fallback value will be used" unless client.initialized?

  detail = client.variation_detail(flag_key, user, false)
  puts "flag=#{flag_key} value=#{detail.value.inspect} variation_id=#{detail.variation_id.inspect} reason=#{detail.reason}"

  client.track(user, "ruby_console_example", 1.0)
  warn "FeatBit events could not be flushed" unless client.flush
ensure
  client.close
end
