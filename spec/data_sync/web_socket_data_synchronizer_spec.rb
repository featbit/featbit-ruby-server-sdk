# frozen_string_literal: true

require "spec_helper"
require "timeout"

RSpec.describe FeatBit::WebSocketDataSynchronizer do
  let(:options) { FeatBit::Options.new(env_secret: "secret") }
  let(:store) { FeatBit::InMemoryDataStore.new }
  let(:status) { FeatBit::StatusProvider.new(logger: options.logger) }

  [FeatBit::Status::STARTING, FeatBit::Status::READY, FeatBit::Status::INTERRUPTED].each do |initial_status|
    it "ignores unknown message types while #{initial_status}" do
      store.init(test_bootstrap(test_flag)) unless initial_status == FeatBit::Status::STARTING
      status.update(initial_status, message: "existing status")
      original_state = [store.initialized?, store.version, store.all_flags]
      socket = instance_double("Socket", close: true)
      attempt = instance_double(FeatBit::WebSocketConnectionAttempt, signal: nil)
      changes = []
      synchronizer = described_class.new(
        options: options, data_store: store, status_provider: status,
        on_flags_changed: ->(key) { changes << key }
      )
      message = JSON.generate(messageType: "future-message", data: { private: "payload" })

      synchronizer.send(:handle_socket_message, socket, message, attempt)

      expect(socket).not_to have_received(:close)
      expect(attempt).not_to have_received(:signal)
      expect([store.initialized?, store.version, store.all_flags]).to eq(original_state)
      expect([status.status, status.message]).to eq([initial_status, "existing status"])
      expect(changes).to be_empty

      synchronizer.send(:handle_socket_message, socket, JSON.generate(test_bootstrap(test_flag)), attempt)

      expect(status.status).to eq(FeatBit::Status::READY)
      expect(store.flag("welcome")).not_to be_nil
      expect(attempt).not_to have_received(:signal)
    end
  end

  it "processes full synchronization messages and reports ready" do
    changes = []
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status, on_flags_changed: lambda { |key|
      changes << key
    })
    result = synchronizer.process_message(test_bootstrap(test_flag))
    expect(result).to be_valid
    expect(result).to be_changed
    expect(store.flag("welcome")).not_to be_nil
    expect(status.status).to eq(FeatBit::Status::READY)
    expect(changes).to include("welcome")
  end

  it "rejects malformed data without raising" do
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)
    result = synchronizer.process_message("not-json")
    expect(result).not_to be_valid
    expect(result).not_to be_changed
    expect(status.status).to eq(FeatBit::Status::STARTING)
  end

  it "applies patches in timestamp order and only reports affected flags" do
    other = test_flag(key: "other")
    expect(store.init(test_bootstrap(test_flag, other), version: 1)).to be(true)
    changes = []
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      on_flags_changed: ->(key) { changes << key }
    )
    patched = test_flag
    patched["name"] = "updated"
    patched["updatedAt"] = "2026-01-02T00:00:00Z"
    message = {
      "messageType" => "data-sync",
      "data" => { "eventType" => "patch", "featureFlags" => [patched], "segments" => [] }
    }
    result = synchronizer.process_message(message)
    expect(result).to be_valid
    expect(result).to be_changed
    expect(store.flag("welcome")["name"]).to eq("updated")
    expect(changes).to eq(["welcome"])
  end

  it "ignores stale patch items without hiding fresh changes" do
    expect(store.init(test_bootstrap(test_flag), version: 10)).to be(true)
    changes = []
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      on_flags_changed: ->(key) { changes << key }
    )
    stale_flag = test_flag
    stale_flag["timestamp"] = 9
    fresh_flag = test_flag(key: "fresh")
    fresh_flag["timestamp"] = 11
    message = {
      "messageType" => "data-sync",
      "data" => { "eventType" => "patch", "featureFlags" => [stale_flag, fresh_flag], "segments" => [] }
    }

    result = synchronizer.process_message(message)
    expect(result).to be_valid
    expect(result).to be_changed
    expect(store.flag("fresh")).not_to be_nil
    expect(changes).to eq(["fresh"])
  end

  it "accepts an all-stale patch without closing the socket" do
    expect(store.init(test_bootstrap(test_flag), version: 10)).to be(true)
    stale_flag = test_flag
    stale_flag["timestamp"] = 9
    message = {
      "messageType" => "data-sync",
      "data" => { "eventType" => "patch", "featureFlags" => [stale_flag], "segments" => [] }
    }
    socket = instance_double("Socket", close: true)
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)

    result = synchronizer.process_message(message)
    synchronizer.send(:handle_socket_message, socket, JSON.generate(message))

    expect(result).to be_valid
    expect(result).not_to be_changed
    expect(socket).not_to have_received(:close)
    expect(store.version).to eq(10)
    expect(status.status).to eq(FeatBit::Status::READY)
  end

  it "accepts an unchanged full synchronization and reports ready" do
    message = test_bootstrap(test_flag)
    expect(store.init(message)).to be(true)
    status.update(FeatBit::Status::INTERRUPTED, message: "disconnected")
    socket = instance_double("Socket", close: true)
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)

    result = synchronizer.process_message(message)
    synchronizer.send(:handle_socket_message, socket, JSON.generate(message))

    expect(result).to be_valid
    expect(result).not_to be_changed
    expect(socket).not_to have_received(:close)
    expect(status.status).to eq(FeatBit::Status::READY)
  end

  it "rejects an entire malformed patch before applying valid siblings" do
    fresh_flag = test_flag(key: "fresh")
    fresh_flag["timestamp"] = 11
    malformed_flag = test_flag
    malformed_flag.delete("key")
    message = {
      "messageType" => "data-sync",
      "data" => { "eventType" => "patch", "featureFlags" => [malformed_flag, fresh_flag], "segments" => [] }
    }
    socket = instance_double("Socket", close: true)
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)

    synchronizer.send(:handle_socket_message, socket, JSON.generate(message))

    expect(socket).not_to have_received(:close)
    expect(store.flag("fresh")).to be_nil
  end

  it "keeps the socket open when a synchronization message is rejected" do
    socket = instance_double("Socket", close: true)
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)

    synchronizer.send(:handle_socket_message, socket, "not-json")

    expect(socket).not_to have_received(:close)
  end

  [false, true].each do |with_attempt|
    it "preserves state and processes later messages after invalid input, with attempt: #{with_attempt}" do
      logger = instance_double(Logger, error: nil)
      configured_options = FeatBit::Options.new(env_secret: "secret", logger: logger)
      socket = instance_double("Socket", close: true)
      attempt = with_attempt ? instance_double(FeatBit::WebSocketConnectionAttempt, signal: nil) : nil
      changes = []
      synchronizer = described_class.new(
        options: configured_options, data_store: store, status_provider: status,
        on_flags_changed: ->(key) { changes << key }
      )
      invalid_messages = [
        "not-json-private-payload", "null", "[]", "{}",
        JSON.generate(messageType: "data-sync", data: nil),
        JSON.generate(messageType: "data-sync", data: { eventType: "unknown", featureFlags: [], segments: [] }),
        JSON.generate(messageType: "data-sync", data: { eventType: "full", featureFlags: {}, segments: [] })
      ]

      [FeatBit::Status::STARTING, FeatBit::Status::READY, FeatBit::Status::INTERRUPTED].each do |state|
        store.init(test_bootstrap(test_flag)) unless state == FeatBit::Status::STARTING
        status.update(state, message: "existing status")
        original = [store.initialized?, store.version, store.all_flags]
        invalid_messages.each { |message| synchronizer.send(:handle_socket_message, socket, message, attempt) }

        expect([store.initialized?, store.version, store.all_flags]).to eq(original)
        expect([status.status, status.message]).to eq([state, "existing status"])
        expect(changes).to be_empty
      end
      expect(logger).to have_received(:error).exactly(invalid_messages.length * 3).times
      expect(logger).not_to have_received(:error).with(include("private-payload"))

      patched = test_flag
      patched["updatedAt"] = "2026-01-02T00:00:00Z"
      patched["name"] = "recovered"
      message = { messageType: "data-sync", data: { eventType: "patch", featureFlags: [patched], segments: [] } }
      synchronizer.send(:handle_socket_message, socket, JSON.generate(message), attempt)

      expect(store.flag("welcome")["name"]).to eq("recovered")
      expect(status.status).to eq(FeatBit::Status::READY)
      expect(changes).to eq(["welcome"])
      expect(socket).not_to have_received(:close)
      expect(attempt).not_to have_received(:signal) if attempt
    end
  end
end
