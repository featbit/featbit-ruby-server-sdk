# frozen_string_literal: true

require "featbit"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.order = :random
end

def test_flag(key: "welcome", type: "string", enabled: true, variations: nil, targets: [], rules: [], fallthrough: nil)
  variations ||= [{ "id" => "on", "value" => type == "boolean" ? "true" : "hello" },
                  { "id" => "off", "value" => type == "boolean" ? "false" : "goodbye" }]
  fallthrough ||= {
    "dispatchKey" => "keyId",
    "variations" => [{ "id" => variations.first["id"], "rollout" => [0, 1] }]
  }
  {
    "id" => "flag-#{key}", "key" => key, "name" => key, "variationType" => type,
    "variations" => variations, "targetUsers" => targets, "rules" => rules,
    "isEnabled" => enabled, "disabledVariationId" => variations.last["id"],
    "fallthrough" => fallthrough, "isArchived" => false,
    "updatedAt" => "2026-01-01T00:00:00Z"
  }
end

def test_bootstrap(*flags, segments: [])
  {
    "messageType" => "data-sync",
    "data" => { "eventType" => "full", "featureFlags" => flags, "segments" => segments }
  }
end
