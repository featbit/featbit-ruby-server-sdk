# frozen_string_literal: true

require "spec_helper"

RSpec.describe FeatBit::Client do
  it "evaluates offline flags, exposes details and tracks no events offline" do
    client = described_class.new(FeatBit::Options.new(offline: true, bootstrap: test_bootstrap(test_flag(type: "boolean"))))
    expect(client).to be_initialized
    expect(client.bool_variation("welcome", { key: "u1" }, false)).to be(true)
    detail = client.variation_detail("welcome", { key: "u1" }, false)
    expect(detail.variation_id).to eq("on")
    expect(client.track({ key: "u1" }, "clicked")).to be(false)
    expect(client.close).to be(true)
  end

  it "serializes event users with the FeatBit wire format" do
    user = FeatBit::User.new("u1", name: "Ada", custom: { country: "cn", score: 7 })

    expect(user.to_h).to eq(
      "keyId" => "u1",
      "name" => "Ada",
      "customizedProperties" => [
        { "name" => "country", "value" => "cn" },
        { "name" => "score", "value" => "7" }
      ]
    )
  end

  it "keeps typed evaluation values but serializes event variation values as strings" do
    events = []
    processor = Object.new
    processor.define_singleton_method(:enqueue) do |event|
      events << event
      true
    end
    processor.define_singleton_method(:close) { true }
    synchronizer = instance_double("Synchronizer", start: true, close: true)
    options = FeatBit::Options.new(
      env_secret: "secret",
      bootstrap: test_bootstrap(test_flag(type: "boolean")),
      start_wait: 0.001,
      synchronizer_factory: ->(*) { synchronizer },
      event_processor_factory: ->(*) { processor }
    )
    client = described_class.new(options)

    expect(client.bool_variation("welcome", { key: "u1" }, false)).to be(true)
    expect(events.dig(0, :variations, 0, :variation, :value)).to eq("true")
    expect(client.close).to be(true)
  end

  it "tracks custom events using the supported metric fields" do
    events = []
    processor = Object.new
    processor.define_singleton_method(:enqueue) do |event|
      events << event
      true
    end
    processor.define_singleton_method(:close) { true }
    synchronizer = instance_double("Synchronizer", start: true, close: true)
    options = FeatBit::Options.new(
      env_secret: "secret",
      start_wait: 0.001,
      synchronizer_factory: ->(*) { synchronizer },
      event_processor_factory: ->(*) { processor }
    )
    client = described_class.new(options)

    expect(client.track({ key: "u1" }, "checkout_completed", 99)).to be(true)
    expect(events.fetch(0)).to include(user: include("keyId" => "u1"))
    expect(events.dig(0, :metrics, 0)).to include(
      eventName: "checkout_completed",
      numericValue: 99.0,
      route: "index/metric",
      type: "CustomEvent",
      appType: "ruby-server-side",
      timestamp: be_a(Integer)
    )
    expect(events.dig(0, :metrics, 0)).not_to have_key(:properties)
    expect(client.close).to be(true)
  end

  it "defensively copies and freezes nested user attributes" do
    key = +"u1"
    name = +"Ada"
    attributes = { profile: { tags: ["beta"] } }
    user = FeatBit::User.new(key, name: name, custom: attributes)
    key.replace("changed")
    name.replace("changed")
    attributes[:profile][:tags] << "mutated"

    expect(user.key).to eq("u1")
    expect(user.name).to eq("Ada")
    expect(user.key).to be_frozen
    expect(user.name).to be_frozen
    expect(user["profile"]).to eq("tags" => ["beta"])
    expect(user["profile"]).to be_frozen
    expect(user["profile"]["tags"]).to be_frozen
  end

  it "never raises from public evaluation methods" do
    broken_store = Object.new
    def broken_store.initialized? = true
    def broken_store.flag(_key) = raise("boom")
    def broken_store.segment(_key) = raise("boom")
    options = FeatBit::Options.new(offline: true, data_store: broken_store)
    client = described_class.new(options)
    expect { client.variation("x", Object.new, "fallback") }.not_to raise_error
    expect(client.variation("x", Object.new, "fallback")).to eq("fallback")
  end

  it "allows listener mutation while notifications run" do
    client = described_class.new(FeatBit::Options.new(offline: true, bootstrap: test_bootstrap(test_flag)))
    received = Queue.new
    id = client.add_flag_change_listener { |key| received << key }
    threads = 10.times.map do
      Thread.new do
        50.times do
          transient = client.add_flag_change_listener { |_key| nil }
          client.remove_flag_change_listener(transient)
        end
      end
    end
    client.send(:broadcast_flag_change, "welcome")
    threads.each(&:join)
    expect(received.pop).to eq("welcome")
    expect(client.remove_flag_change_listener(id)).to be(true)
  end

  it "starts online components when offline and disable_events are false" do
    synchronizer = instance_double("Synchronizer", start: true, close: true)
    processor = instance_double("EventProcessor", close: true)
    options = FeatBit::Options.new(
      env_secret: "secret",
      start_wait: 0.001,
      offline: false,
      disable_events: false,
      synchronizer_factory: ->(*) { synchronizer },
      event_processor_factory: ->(*) { processor }
    )

    client = described_class.new(options)

    expect(options.offline).to be(false)
    expect(options.disable_events).to be(false)
    expect(synchronizer).to have_received(:start)
    expect(client.event_processor).to be(processor)
    expect(client.close).to be(true)
  end

  it "does not start event delivery for invalid online options" do
    client = described_class.new(FeatBit::Options.new)

    expect(client.event_processor).to be_a(FeatBit::NullEventProcessor)
    expect(client.status_provider.status).to eq(FeatBit::Status::FAILED)
    expect(client.close).to be(true)
  end

  it "remains initialized while evaluating cached data after interruption" do
    client = described_class.new(FeatBit::Options.new(offline: true, bootstrap: test_bootstrap(test_flag)))
    client.status_provider.update(FeatBit::Status::INTERRUPTED)

    expect(client).to be_initialized
    expect(client.string_variation("welcome", { key: "u1" }, "fallback")).to eq("hello")
    expect(client.close).to be(true)
  end

  it "reports a reentrant close as incomplete without deadlocking" do
    client = nil
    reentrant_results = []
    synchronizer = instance_double("Synchronizer", start: true, close: true)
    processor = Object.new
    processor.define_singleton_method(:close) do
      reentrant_results << client.close
      true
    end
    options = FeatBit::Options.new(
      env_secret: "secret",
      start_wait: 0.001,
      synchronizer_factory: ->(*) { synchronizer },
      event_processor_factory: ->(*) { processor }
    )
    client = described_class.new(options)

    expect(client.close).to be(true)
    expect(reentrant_results).to eq([false])
  end

  it "retries failed shutdown without skipping components or raising" do
    synchronizer = instance_double("Synchronizer", start: true)
    processor = instance_double("EventProcessor")
    allow(synchronizer).to receive(:close).and_raise("synchronizer failed")
    allow(processor).to receive(:close).and_return(true)
    options = FeatBit::Options.new(
      env_secret: "secret",
      start_wait: 0.001,
      synchronizer_factory: ->(*) { synchronizer },
      event_processor_factory: ->(*) { processor }
    )
    client = described_class.new(options)

    expect(client.close).to be(false)
    expect(client.close).to be(false)
    expect(synchronizer).to have_received(:close).twice
    expect(processor).to have_received(:close).twice
    expect(client.status_provider.status).to eq(FeatBit::Status::CLOSED)
  end

  it "can complete a previously incomplete close and caches only success" do
    synchronizer = instance_double("Synchronizer", start: true)
    allow(synchronizer).to receive(:close).and_return(false, true)
    client = described_class.new(FeatBit::Options.new(
                                   env_secret: "secret", disable_events: true, start_wait: 0.001,
                                   synchronizer_factory: ->(*) { synchronizer }
                                 ))

    expect(client.close).to be(false)
    expect(client.close).to be(true)
    expect(client.close).to be(true)
    expect(synchronizer).to have_received(:close).twice
  end

  it "does not report concurrent or status-listener shutdown as completed" do
    entered = Queue.new
    release = Queue.new
    synchronizer = instance_double("Synchronizer", start: true)
    allow(synchronizer).to receive(:close) do
      entered << true
      release.pop
      true
    end
    options = FeatBit::Options.new(
      env_secret: "secret", disable_events: true, start_wait: 0.001, synchronizer_factory: ->(*) { synchronizer }
    )
    client = described_class.new(options)
    nested = []
    client.status_provider.add_listener { |state, _| nested << client.close if state == FeatBit::Status::CLOSED }
    closer = Thread.new { client.close }
    Timeout.timeout(2) { entered.pop }

    expect(client.close).to be(false)
    release << true
    expect(closer.join(2).value).to be(true)
    expect(nested).to eq([false])
    expect(client.close).to be(true)
    expect(synchronizer).to have_received(:close).once
  ensure
    release << true if release
    closer&.join(2)
  end
end
