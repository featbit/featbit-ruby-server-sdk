# frozen_string_literal: true

require "json"
require "timeout"
require "featbit"

secret = ENV.fetch("FEATBIT_ENV_SECRET")
flag_key = ENV.fetch("FEATBIT_FLAG_KEY")
options = FeatBit::Options.new(
  env_secret: secret,
  streaming_url: ENV.fetch("FEATBIT_STREAMING_URL", "wss://app-eval.featbit.co"),
  event_url: ENV.fetch("FEATBIT_EVENT_URL", "https://app-eval.featbit.co"),
  start_wait: 15,
  connect_timeout: 10,
  read_timeout: 15
)

baseline_thread_ids = Thread.list.map(&:object_id)
client = FeatBit::Client.new(options)
raise "FeatBit client did not become ready: #{client.status_provider.message}" unless client.initialized?

user = FeatBit::User.new(
  ENV.fetch("FEATBIT_USER_KEY", "ruby-live-review-user"),
  name: "Ruby SDK Live Review",
  custom: { sdk: "ruby", check: "live-integration" }
)
detail = client.variation_detail(flag_key, user, false)
raise "live flag evaluation failed: #{detail.error_message}" unless detail.success?
raise "live flag is not boolean: #{detail.value.class}" unless [true, false].include?(detail.value)

errors = Queue.new
thread_count = Integer(ENV.fetch("FEATBIT_LIVE_THREADS", "8"), 10)
evaluations_per_thread = Integer(ENV.fetch("FEATBIT_LIVE_EVALUATIONS_PER_THREAD", "5"), 10)
evaluation_count = thread_count * evaluations_per_thread
thread_count.times.map do |index|
  Thread.new do
    thread_user = FeatBit::User.new("ruby-live-review-#{index}")
    evaluations_per_thread.times do
      value = client.bool_variation(flag_key, thread_user, false)
      errors << "non-boolean value" unless [true, false].include?(value)
    rescue StandardError => e
      errors << e.message
    end
  end
end.each(&:join)
raise "concurrent live evaluations failed: #{errors.size}" unless errors.empty?

track_result = client.track(user, "ruby_sdk_live_review", 1.0)
flush_result = client.flush
ready_status = client.status_provider.status
close_result = client.close
raise "remote track was not accepted by the local event queue" unless track_result
raise "remote event flush failed" unless flush_result
raise "client did not close cleanly" unless close_result

Timeout.timeout(5) do
  loop do
    residual = Thread.list.reject { |thread| baseline_thread_ids.include?(thread.object_id) }
                          .select { |thread| thread.name&.start_with?("featbit-") && thread.alive? }
    break if residual.empty?

    sleep(0.05)
  end
end

puts JSON.generate(
  initialized: true,
  synchronized_flag: flag_key,
  value: detail.value,
  variation_id: detail.variation_id,
  reason: detail.reason,
  concurrent_evaluations: evaluation_count,
  status_before_close: ready_status,
  track: track_result,
  flush: flush_result,
  close: close_result,
  residual_worker_threads: 0
)
