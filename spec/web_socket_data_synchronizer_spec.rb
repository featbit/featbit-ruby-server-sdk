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

    expect(socket).to have_received(:close)
    expect(store.flag("fresh")).to be_nil
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

  it "registers handlers before a connector can emit open" do
    socket = Class.new do
      attr_reader :handlers, :sent

      def initialize
        @handlers = {}
        @sent = []
        @closed = false
      end

      def on(event, &block) = @handlers[event] = block
      def send(message) = @sent << message
      def closed? = @closed
      def close = @closed = true
    end.new
    connected = Queue.new
    connector = lambda do |_url, _headers, &configure|
      configure.call(socket)
      socket.handlers.fetch(:open).call
      connected << true
      socket
    end
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      connector: connector
    )

    synchronizer.start

    Timeout.timeout(2) { connected.pop }
    expect(JSON.parse(socket.sent.fetch(0))).to eq("messageType" => "data-sync", "data" => { "timestamp" => 0 })
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

  it "times out when the WebSocket handshake never opens" do
    closed = Queue.new
    socket = Class.new do
      def initialize(closed)
        @closed_event = closed
        @handlers = {}
        @closed = false
      end

      def on(event, &block) = @handlers[event] = block
      def closed? = @closed

      def close
        return true if @closed

        @closed = true
        @closed_event << true
        true
      end
    end.new(closed)
    timeout_options = FeatBit::Options.new(env_secret: "secret", connect_timeout: 0.05, reconnect_delay: 10)
    synchronizer = described_class.new(
      options: timeout_options,
      data_store: store,
      status_provider: status,
      connector: ->(*) { socket }
    )

    synchronizer.start

    expect(Timeout.timeout(2) { closed.pop }).to be(true)
    expect(status.status).to eq(FeatBit::Status::INTERRUPTED)
    expect(status.message).to eq("WebSocket handshake timed out")
    expect(synchronizer.close).to be(true)
  end

  it "backs off consecutive failures that happen before the WebSocket opens" do
    delays = Queue.new
    sockets = []
    connector = lambda do |_url, _headers, &configure|
      socket = Class.new do
        attr_reader :handlers

        def initialize
          @handlers = {}
          @closed = false
        end

        def on(event, &block) = @handlers[event] = block
        def closed? = @closed
        def close = @closed = true
      end.new
      sockets << socket
      configure.call(socket)
      Thread.new { socket.handlers.fetch(:error).call(StandardError.new("handshake failed")) }
      socket
    end
    synchronizer = described_class.new(
      options: options,
      data_store: store,
      status_provider: status,
      connector: connector
    )
    delay_count = 0
    allow(synchronizer).to receive(:interruptible_sleep) do |duration|
      delay_count += 1
      delays << duration
      synchronizer.close if delay_count == 2
    end

    synchronizer.start

    expect(Timeout.timeout(2) { delays.pop }).to eq(1.0)
    expect(Timeout.timeout(2) { delays.pop }).to eq(2.0)
    expect(synchronizer.close).to be(true)
    expect(sockets.length).to eq(2)
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

    Timeout.timeout(2) { sleep(0.01) until fake_socket.closed? }
    expect(fake_socket).to be_closed
    expect(synchronizer.close).to be(true)
  end

  it "stops reconnecting when the server rejects the connection with close code 4003" do
    raw_frame = WebSocket::Frame::Outgoing::Server.new(
      version: 13, type: :close, code: 4003, data: "invalid environment secret"
    ).to_s
    close_frame = WebSocket::Frame::Incoming::Client.new(version: 13, data: raw_frame).next
    attempts = 0
    socket = Class.new do
      attr_reader :handlers

      def initialize
        @handlers = {}
        @closed = false
      end

      def on(event, &block) = @handlers[event] = block
      def send(*) = nil
      def closed? = @closed
      def close = @closed = true
    end.new
    connector = lambda do |_url, _headers, &configure|
      attempts += 1
      configure.call(socket)
      socket
    end
    rejection_options = FeatBit::Options.new(env_secret: "secret", reconnect_delay: 0.01)
    synchronizer = described_class.new(
      options: rejection_options,
      data_store: store,
      status_provider: status,
      connector: connector
    )

    synchronizer.start
    Timeout.timeout(2) { sleep(0.01) until socket.handlers.key?(:message) }
    socket.handlers.fetch(:open).call
    socket.handlers.fetch(:message).call(close_frame)
    worker = synchronizer.instance_variable_get(:@lifecycle).instance_variable_get(:@thread)
    expect(worker.join(2)).to eq(worker)

    expect(attempts).to eq(1)
    expect(socket).to be_closed
    expect(status.status).to eq(FeatBit::Status::FAILED)
    expect(status.message).to eq("WebSocket connection rejected by server (4003): invalid environment secret")
    expect(synchronizer.close).to be(true)
  end

  it "reconnects after a non-rejected close frame" do
    raw_frame = WebSocket::Frame::Outgoing::Server.new(
      version: 13, type: :close, code: 1000, data: "service restart"
    ).to_s
    close_frame = WebSocket::Frame::Incoming::Client.new(version: 13, data: raw_frame).next
    connected = Queue.new
    connector = lambda do |_url, _headers, &configure|
      socket = Class.new do
        attr_reader :handlers

        def initialize
          @handlers = {}
          @closed = false
        end

        def on(event, &block) = @handlers[event] = block
        def send(*) = nil
        def closed? = @closed
        def close = @closed = true
      end.new
      configure.call(socket)
      connected << socket
      socket
    end
    reconnect_options = FeatBit::Options.new(env_secret: "secret", reconnect_delay: 0.01)
    synchronizer = described_class.new(
      options: reconnect_options,
      data_store: store,
      status_provider: status,
      connector: connector
    )

    synchronizer.start
    first_socket = Timeout.timeout(2) { connected.pop }
    first_socket.handlers.fetch(:open).call
    first_socket.handlers.fetch(:message).call(close_frame)
    second_socket = Timeout.timeout(2) { connected.pop }

    expect(first_socket).to be_closed
    expect(second_socket).not_to be_closed
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
