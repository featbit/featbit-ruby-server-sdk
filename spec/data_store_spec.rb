# frozen_string_literal: true

require "spec_helper"

RSpec.describe FeatBit::InMemoryDataStore do
  it "initializes immutable snapshots and returns defensive copies" do
    store = described_class.new
    expect(store.init(test_bootstrap(test_flag))).to be(true)
    first = store.flag("welcome")
    first["name"] = "changed"
    expect(store.flag("welcome")["name"]).to eq("welcome")
  end

  it "accepts only newer patch versions" do
    store = described_class.new
    store.init(test_bootstrap(test_flag), version: 10)
    expect(store.upsert(:flags, test_flag, version: 9)).to be(false)
    expect(store.upsert(:flags, test_flag, version: 11)).to be(true)
  end

  it "accepts equal timestamps for different entities" do
    store = described_class.new
    store.init(test_bootstrap(test_flag), version: 10)

    expect(store.upsert(:flags, test_flag(key: "first"), version: 11)).to be(true)
    expect(store.upsert(:flags, test_flag(key: "second"), version: 11)).to be(true)
    expect(store.all_flags.keys).to include("first", "second")
  end

  it "is safe under concurrent readers and writers" do
    store = described_class.new
    store.init(test_bootstrap(test_flag), version: 1)
    errors = Queue.new
    threads = 8.times.map do |index|
      Thread.new do
        200.times do |iteration|
          store.flag("welcome")
          store.all_flags
          store.upsert(:flags, test_flag(key: "flag-#{index}"), version: 2 + (index * 200) + iteration)
        rescue StandardError => e
          errors << e
        end
      end
    end
    threads.each(&:join)
    expect(errors).to be_empty
  end
end
