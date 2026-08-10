# frozen_string_literal: true

require "spec_helper"
require "timeout"

RSpec.describe FeatBit::WebSocketDataSynchronizer do
  let(:options) { FeatBit::Options.new(env_secret: "secret") }
  let(:store) { FeatBit::InMemoryDataStore.new }
  let(:status) { FeatBit::StatusProvider.new(logger: options.logger) }

  it "processes full synchronization messages and reports ready" do
    changes = []
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status, on_flags_changed: lambda { |key|
      changes << key
    })
    expect(synchronizer.process_message(test_bootstrap(test_flag))).to be(true)
    expect(store.flag("welcome")).not_to be_nil
    expect(status.status).to eq(FeatBit::Status::READY)
    expect(changes).to include("welcome")
  end

  it "rejects malformed data without raising" do
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)
    expect { synchronizer.process_message("not-json") }.not_to raise_error
    expect(status.status).to eq(FeatBit::Status::FAILED)
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
    expect(synchronizer.process_message(message)).to be(true)
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

    expect(synchronizer.process_message(message)).to be(true)
    expect(store.flag("fresh")).not_to be_nil
    expect(changes).to eq(["fresh"])
  end

  it "closes a socket when setup fails before reconnecting" do
    closed = Queue.new
    fake_socket = Class.new do
      def initialize(closed)
        @closed = closed
      end

      def on(*) = raise("handler setup failed")

      def close
        @closed << true
        true
      end
    end.new(closed)
    slow_options = FeatBit::Options.new(env_secret: "secret", reconnect_delay: 10)
    synchronizer = described_class.new(
      options: slow_options,
      data_store: store,
      status_provider: status,
      connector: ->(*) { fake_socket }
    )

    synchronizer.start

    expect(Timeout.timeout(2) { closed.pop }).to be(true)
    expect(synchronizer.close).to be(true)
  end

  it "closes the socket when a synchronization message is rejected" do
    socket = instance_double("Socket", close: true)
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)

    synchronizer.send(:handle_socket_message, socket, "not-json")

    expect(socket).to have_received(:close)
  end

  it "connects with FeatBit authentication and requests the local version" do
    fake_socket = Class.new do
      attr_reader :handlers, :sent

      def initialize
        @handlers = {}
        @sent = []
        @closed = false
      end

      def on(event, &block)
        @handlers[event] = block
      end

      def send(message)
        @sent << message
      end

      def emit(event, payload = nil)
        instance_exec(payload, &@handlers.fetch(event))
      end

      def closed?
        @closed
      end

      def close
        @closed = true
      end
    end.new
    connection = Queue.new
    connector = lambda do |url, headers|
      connection << [url, headers]
      fake_socket
    end
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      connector: connector
    )
    expect(synchronizer.start).to be(true)
    url, headers = Timeout.timeout(2) { connection.pop }
    Timeout.timeout(2) { sleep(0.01) until fake_socket.handlers.key?(:open) }
    fake_socket.emit(:open)
    expect(url).to match(%r{\Awss://app-eval\.featbit\.co/streaming\?token=.+&type=server\z})
    expect(headers["Authorization"]).to eq("secret")
    expect(JSON.parse(fake_socket.sent.fetch(0))).to eq("messageType" => "data-sync", "data" => { "timestamp" => 0 })
    expect(synchronizer.close).to be(true)
  end

  it "interrupts reconnect backoff promptly when closed" do
    attempted = Queue.new
    connector = lambda do |_url, _headers|
      attempted << true
      raise "offline"
    end
    slow_options = FeatBit::Options.new(env_secret: "secret", reconnect_delay: 10)
    synchronizer = described_class.new(
      options: slow_options,
      data_store: store,
      status_provider: status,
      connector: connector
    )
    expect(synchronizer.start).to be(true)
    Timeout.timeout(2) { attempted.pop }
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    expect(synchronizer.close).to be(true)
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
  end

  it "closes a failed socket so the reconnect loop can recover" do
    fake_socket = Class.new do
      attr_reader :handlers

      def initialize
        @handlers = {}
        @closed = false
      end

      def on(event, &block) = @handlers[event] = block
      def closed? = @closed
      def close = @closed = true
    end.new
    connected = Queue.new
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      connector: lambda { |*|
        connected << true
        fake_socket
      }
    )
    synchronizer.start
    Timeout.timeout(2) { connected.pop }
    Timeout.timeout(2) { sleep(0.01) until fake_socket.handlers.key?(:error) }

    fake_socket.handlers.fetch(:error).call(StandardError.new("connection failed"))

    expect(fake_socket).to be_closed
    expect(synchronizer.close).to be(true)
  end

  it "encodes a current timestamp into the connection token without falling back to the secret" do
    secret = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG="
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)
    token = synchronizer.send(:build_token, secret)
    reverse_alphabet = described_class::ALPHABETS.invert
    decode = lambda do |encoded|
      Integer(encoded.chars.map { |character| reverse_alphabet.fetch(character) }.join, 10)
    end

    start = decode.call(token[0, 3])
    timestamp_length = decode.call(token[3, 2])
    body = token[5..]
    timestamp = decode.call(body[start, timestamp_length])
    reconstructed_secret = body[0, start] + body[(start + timestamp_length)..]

    expect(token).not_to eq(secret)
    expect(reconstructed_secret).to eq(secret.delete_suffix("="))
    expect((Time.now.to_f * 1000).round - timestamp).to be_between(0, 2_000)
  end

  it "does not expose the raw environment secret when token encoding fails" do
    synchronizer = described_class.new(options: options, data_store: store, status_provider: status)
    allow(SecureRandom).to receive(:random_number).and_raise("random source failed")

    expect { synchronizer.send(:build_token, "server-secret") }.to raise_error("random source failed")
  end
end
