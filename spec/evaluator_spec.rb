# frozen_string_literal: true

require "spec_helper"

RSpec.describe FeatBit::Evaluator do
  let(:store) { FeatBit::InMemoryDataStore.new }
  let(:evaluator) do
    described_class.new(flag_getter: ->(key) { store.flag(key) }, segment_getter: ->(id) { store.segment(id) })
  end

  it "returns the disabled variation and ID" do
    store.init(test_bootstrap(test_flag(enabled: false)))
    detail = evaluator.evaluate("welcome", FeatBit::User.new("u1"), "fallback")
    expect(detail.value).to eq("goodbye")
    expect(detail.variation_id).to eq("off")
    expect(detail.reason).to eq("flag off")
    expect(detail.send_to_experiment).to be(false)
  end

  it "matches explicit targets before rules" do
    flag = test_flag(targets: [{ "keyIds" => ["u1"], "variationId" => "off" }])
    flag["exptIncludeAllTargets"] = true
    store.init(test_bootstrap(flag))
    detail = evaluator.evaluate("welcome", FeatBit::User.new("u1"), "fallback")
    expect(detail.value).to eq("goodbye")
    expect(detail.reason).to eq("target match")
    expect(detail.send_to_experiment).to be(true)
  end

  it "supports attributes, rules, segments and typed JSON" do
    segment = {
      "id" => "beta", "included" => [], "excluded" => [], "isArchived" => false,
      "updatedAt" => "2026-01-01T00:00:00Z",
      "rules" => [{ "conditions" => [{ "property" => "country", "op" => "Equal", "value" => "cn" }] }]
    }
    variations = [{ "id" => "on", "value" => '{"layout":"new"}' }, { "id" => "off", "value" => "{}" }]
    rules = [{
      "conditions" => [{ "property" => "User is in segment", "op" => nil, "value" => '["beta"]' }],
      "variations" => [{ "id" => "on", "rollout" => [0, 1] }]
    }]
    store.init(test_bootstrap(test_flag(type: "json", variations: variations, rules: rules), segments: [segment]))
    detail = evaluator.evaluate("welcome", FeatBit::User.new("u1", custom: { country: "cn" }), {})
    expect(detail.value).to eq("layout" => "new")
    expect(detail.reason).to eq("rule match")
  end

  it "returns safe error details for missing flags and users" do
    store.init(test_bootstrap(test_flag))
    expect(evaluator.evaluate("missing", FeatBit::User.new("u1"), "fallback").error_kind).to eq(:flag_not_found)
    expect(evaluator.evaluate("welcome", FeatBit::User.new(""), "fallback").error_kind).to eq(:user_not_specified)
  end

  it "rejects malformed typed variations instead of coercing them" do
    malformed = test_flag(type: "boolean", variations: [{ "id" => "on", "value" => "not-a-boolean" }])
    malformed["disabledVariationId"] = "on"
    store.init(test_bootstrap(malformed))
    detail = evaluator.evaluate("welcome", FeatBit::User.new("u1"), false)
    expect(detail.value).to be(false)
    expect(detail.error_kind).to eq(:error)
  end

  it "terminates safely when segments reference each other" do
    segment_condition = lambda do |segment_id|
      { "property" => "User is in segment", "op" => nil, "value" => JSON.generate([segment_id]) }
    end
    segments = {
      "a" => { "id" => "a", "included" => [], "excluded" => [], "rules" => [{ "conditions" => [segment_condition.call("b")] }] },
      "b" => { "id" => "b", "included" => [], "excluded" => [], "rules" => [{ "conditions" => [segment_condition.call("a")] }] }
    }
    flag = test_flag(
      rules: [{ "conditions" => [segment_condition.call("a")], "variations" => [{ "id" => "off", "rollout" => [0, 1] }] }]
    )
    store.init(test_bootstrap(flag, segments: segments.values))

    detail = evaluator.evaluate("welcome", FeatBit::User.new("u1"), "fallback")

    expect(detail.value).to eq("hello")
    expect(detail.reason).to eq(FeatBit::Evaluator::REASONS[:fallthrough])
  end
end
