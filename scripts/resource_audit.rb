# frozen_string_literal: true

require "json"
require "featbit"

def bootstrap
  variation = { "id" => "on", "value" => "true" }
  {
    "messageType" => "data-sync",
    "data" => {
      "eventType" => "full",
      "segments" => [],
      "featureFlags" => [{
        "id" => "audit-flag",
        "key" => "resource-audit",
        "name" => "Resource Audit",
        "variationType" => "boolean",
        "variations" => [variation],
        "targetUsers" => [],
        "rules" => [],
        "isEnabled" => true,
        "disabledVariationId" => "on",
        "fallthrough" => { "variations" => [{ "id" => "on", "rollout" => [0, 1] }] },
        "isArchived" => false,
        "updatedAt" => "2026-01-01T00:00:00Z"
      }]
    }
  }
end

def named_worker_threads
  Thread.list.select { |thread| thread.name&.start_with?("featbit-") && thread.alive? }
end

baseline_thread_ids = named_worker_threads.map(&:object_id)
options = FeatBit::Options.new(env_secret: "audit", events_flush_interval: 60)

100.times do
  processor = FeatBit::EventProcessor.new(options, sender: ->(_batch) { true })
  raise "event processor did not close" unless processor.close
end

client = FeatBit::Client.new(FeatBit::Options.new(offline: true, bootstrap: bootstrap))
user = FeatBit::User.new("resource-audit-user", custom: { cohort: "audit" })
errors = Queue.new
evaluation_count = 16 * 5_000

5_000.times { client.bool_variation("resource-audit", user, false) }
GC.start(full_mark: true, immediate_sweep: true)
baseline_heap_slots = GC.stat(:heap_live_slots)

16.times.map do
  Thread.new do
    5_000.times do
      value = client.bool_variation("resource-audit", user, false)
      errors << "wrong value" unless value == true
    rescue StandardError => e
      errors << e
    end
  end
end.each(&:join)

client.close
GC.start(full_mark: true, immediate_sweep: true)
final_heap_slots = GC.stat(:heap_live_slots)
retained_heap_slots = final_heap_slots - baseline_heap_slots
allowed_retained_slots = [10_000, (baseline_heap_slots * 0.2).ceil].max

leaked_threads = named_worker_threads.reject { |thread| baseline_thread_ids.include?(thread.object_id) }
raise "concurrent evaluation errors: #{errors.size}" unless errors.empty?
raise "worker threads leaked: #{leaked_threads.map(&:name).join(', ')}" unless leaked_threads.empty?
raise "possible heap retention: #{retained_heap_slots} slots" if retained_heap_slots > allowed_retained_slots

puts JSON.generate(
  evaluations: evaluation_count,
  concurrent_errors: errors.size,
  leaked_worker_threads: leaked_threads.size,
  baseline_heap_slots: baseline_heap_slots,
  final_heap_slots: final_heap_slots,
  retained_heap_slots: retained_heap_slots,
  allowed_retained_slots: allowed_retained_slots,
  result: "ok"
)
