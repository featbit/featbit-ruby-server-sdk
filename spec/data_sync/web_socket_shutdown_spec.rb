# frozen_string_literal: true

require "spec_helper"
require "socket"
require "timeout"

class ShutdownTestSocket
  attr_reader :handlers, :sent, :close_calls

  def initialize
    @handlers = {}
    @sent = []
    @close_calls = 0
  end

  def on(name, &block) = @handlers[name] = block
  def send(message) = @sent << message
  def closed? = @close_calls.positive?

  def close
    @close_calls += 1
    @handlers[:close]&.call(nil)
    true
  end

  def emit(name, event = nil) = @handlers.fetch(name).call(event)
end

RSpec.describe "WebSocket shutdown" do
  let(:store) { FeatBit::InMemoryDataStore.new }
  let(:status) { FeatBit::StatusProvider.new }
  let(:socket) { ShutdownTestSocket.new }
  let(:connected) { Queue.new }
  let(:options) { FeatBit::Options.new(env_secret: "secret", connect_timeout: 30, reconnect_delay: 0.01) }

  def build_sync(connector:, **args)
    @synchronizer = FeatBit::WebSocketDataSynchronizer.new(
      options: options, data_store: store, status_provider: status, connector: connector, **args
    )
  end

  def wait_for(queue) = Timeout.timeout(2) { queue.pop }

  def configured_connector
    lambda do |*, &configure|
      configure.call(socket)
      connected << Thread.current
      socket
    end
  end

  after { @synchronizer&.close }

  it "can close before start and cannot subsequently restart" do
    sync = build_sync(connector: configured_connector)
    expect(sync.close).to be(true)
    expect(sync.close).to be(true)
    expect(sync.start).to be(false)
    expect(connected).to be_empty
  end

  it "does not claim successful cleanup or reconnect after a socket close failure" do
    allow(socket).to receive(:close).and_return(false)
    sync = build_sync(connector: configured_connector)
    sync.start
    wait_for(connected)

    expect(sync.close).to be(false)
    expect(sync.close).to be(false)
    expect(sync.start).to be(false)
    expect(connected).to be_empty
    expect(socket).to have_received(:close).once
  end

  it "cancels a connector that has not returned or published a socket" do
    sync = build_sync(connector: lambda { |*|
      connected << Thread.current
      Queue.new.pop
    })
    sync.start
    connector_thread = wait_for(connected)

    expect(Timeout.timeout(2) { sync.close }).to be(true)
    expect(connector_thread).not_to be_alive
    expect(sync.close).to be(true)
    expect(sync.start).to be(false)
  end

  it "closes a partially connected socket only after its connector has exited" do
    connector_thread = nil
    allow(socket).to receive(:close).and_wrap_original do |original|
      expect(connector_thread).not_to be_alive
      original.call
    end
    sync = build_sync(connector: lambda { |*, &configure|
      configure.call(socket)
      connected << Thread.current
      Queue.new.pop
    })
    sync.start
    connector_thread = wait_for(connected)

    expect(Timeout.timeout(2) { sync.close }).to be(true)
    expect(socket.close_calls).to eq(1)
  end

  it "interrupts handshake waiting and ignores every late callback" do
    sync = build_sync(connector: configured_connector)
    sync.start
    wait_for(connected)
    expect(Timeout.timeout(2) { sync.close }).to be(true)
    status.update(FeatBit::Status::CLOSED)

    socket.emit(:open)
    socket.emit(:message, JSON.generate(test_bootstrap(test_flag)))
    socket.emit(:error, StandardError.new("late error"))
    socket.emit(:close, Struct.new(:code, :reason).new(4003, "late rejection"))

    expect(socket.sent).to be_empty
    expect(store).not_to be_initialized
    expect(status.status).to eq(FeatBit::Status::CLOSED)
    expect(socket.close_calls).to eq(1)
  end

  it "waits for an in-flight callback without killing it or reporting early success" do
    entered = Queue.new
    release = Queue.new
    stub_const("FeatBit::WebSocketLifecycle::CLOSE_WAIT", 0.05)
    sync = build_sync(connector: configured_connector, on_flags_changed: lambda { |_|
      entered << true
      release.pop
    })
    sync.start
    wait_for(connected)
    socket.emit(:open)
    callback = Thread.new { socket.emit(:message, JSON.generate(test_bootstrap(test_flag))) }
    wait_for(entered)

    expect(sync.close).to be(false)
    expect(callback).to be_alive
    expect(socket.close_calls).to eq(0)
    release << true
    expect(callback.join(2)).to eq(callback)
    expect(Timeout.timeout(2) { sleep(0.01) until sync.close }).to be_nil
    expect(status.status).not_to eq(FeatBit::Status::READY)
    expect(socket.close_calls).to eq(1)
  ensure
    release << true if release
    callback&.join(2)
  end

  it "allows a callback to request close without joining itself or continuing notifications" do
    calls = []
    close_results = []
    sync = build_sync(connector: configured_connector, on_flags_changed: lambda { |key|
      calls << key
      close_results << @synchronizer.close
      status.update(FeatBit::Status::CLOSED)
    })
    sync.start
    wait_for(connected)
    socket.emit(:open)
    socket.emit(:message, JSON.generate(test_bootstrap(test_flag, test_flag(key: "second"))))

    expect(close_results).to eq([false])
    expect(calls).to eq(["welcome"])
    expect(Timeout.timeout(2) { sync.close }).to be(true)
    expect(status.status).to eq(FeatBit::Status::CLOSED)
  end

  it "ignores callbacks from an old connection after reconnecting" do
    sockets = Queue.new
    sync = build_sync(connector: lambda { |*, &configure|
      candidate = ShutdownTestSocket.new
      configure.call(candidate)
      sockets << candidate
      candidate
    })
    sync.start
    first = wait_for(sockets)
    first.emit(:open)
    first.emit(:error, StandardError.new("disconnect"))
    second = wait_for(sockets)
    second.emit(:open)
    second.emit(:message, JSON.generate(test_bootstrap(test_flag)))

    first.emit(:error, StandardError.new("old error"))
    first.emit(:close, Struct.new(:code).new(4003))
    expect(status.status).to eq(FeatBit::Status::READY)
    expect(second).not_to be_closed
  end

  %w[ws wss].each do |scheme|
    it "cancels a real stalled #{scheme == 'wss' ? 'TLS' : 'HTTP Upgrade'} connection" do
      server = TCPServer.new("127.0.0.1", 0)
      accepted = Queue.new
      eof = Queue.new
      peer = nil
      server_thread = Thread.new do
        peer = server.accept
        peer.readpartial(4096) # Observe the actual ClientHello or Upgrade request.
        accepted << true
        peer.read # Never send TLS/Upgrade response; wait for client EOF.
        eof << true
      end
      real_options = FeatBit::Options.new(
        env_secret: "secret", streaming_url: "#{scheme}://127.0.0.1:#{server.addr[1]}", connect_timeout: 30
      )
      @synchronizer = FeatBit::WebSocketDataSynchronizer.new(options: real_options, data_store: store, status_provider: status)
      @synchronizer.start
      wait_for(accepted)
      attempt = @synchronizer.instance_variable_get(:@lifecycle).instance_variable_get(:@attempt)
      actual_socket = attempt.socket
      connector_thread = attempt.instance_variable_get(:@connector_thread)

      expect(Timeout.timeout(2) { @synchronizer.close }).to be(true)
      expect(wait_for(eof)).to be(true)
      expect(connector_thread).not_to be_alive
      expect(actual_socket.thread&.alive?).not_to be(true)
      expect(server_thread.join(2)).to eq(server_thread)
    ensure
      @synchronizer&.close
      peer&.close
      server&.close
      server_thread&.kill
      server_thread&.join
    end
  end

  it "returns from close inside a real receive callback before draining the reader" do
    server = TCPServer.new("127.0.0.1", 0)
    completed = Queue.new
    peer = nil
    server_thread = Thread.new do
      peer = server.accept
      handshake = WebSocket::Handshake::Server.new
      handshake << peer.readpartial(4096) until handshake.finished?
      peer.write(handshake.to_s)
      peer.write(WebSocket::Frame::Outgoing::Server.new(
        version: 13, type: :text, data: JSON.generate(test_bootstrap(test_flag))
      ).to_s)
      peer.read
    end
    real_options = FeatBit::Options.new(env_secret: "secret", streaming_url: "ws://127.0.0.1:#{server.addr[1]}")
    @synchronizer = FeatBit::WebSocketDataSynchronizer.new(
      options: real_options, data_store: store, status_provider: status,
      on_flags_changed: ->(_) { completed << [@synchronizer.close, Thread.current] }
    )
    @synchronizer.start

    result, reader = wait_for(completed)
    expect(result).to be(false)
    expect(Timeout.timeout(2) { @synchronizer.close }).to be(true)
    expect(reader).not_to be_alive
    expect(server_thread.join(2)).to eq(server_thread)
  ensure
    @synchronizer&.close
    peer&.close
    server&.close
    server_thread&.kill
    server_thread&.join
  end

  it "forces transport cleanup when sending the close frame stalls" do
    stub_const("FeatBit::ClosableWebSocketClient::CLOSE_TIMEOUT", 0.05)
    client = FeatBit::ClosableWebSocketClient.new
    transport = instance_double(TCPSocket, close: nil)
    reader = Thread.new { sleep }
    client.instance_variable_set(:@socket, transport)
    client.instance_variable_set(:@thread, reader)
    allow(client).to receive(:send) { Queue.new.pop }

    expect(Timeout.timeout(2) { client.close(drain: true) }).to be(true)
    expect(transport).to have_received(:close)
    expect(reader).not_to be_alive
  ensure
    reader&.kill
    reader&.join
  end

  it "defers library-initiated reader shutdown to the owner" do
    client = FeatBit::ClosableWebSocketClient.new
    transport = instance_double(TCPSocket, close: nil)
    client.instance_variable_set(:@socket, transport)
    returned = Queue.new
    reader = Thread.new do
      client.instance_variable_set(:@thread, Thread.current)
      returned << client.close
      sleep
    end

    expect(wait_for(returned)).to be(true)
    expect(reader).to be_alive
    expect(transport).not_to have_received(:close)
    expect(client.close(drain: true)).to be(true)
    expect(transport).to have_received(:close)
    expect(reader).not_to be_alive
  ensure
    reader&.kill
    reader&.join
  end
end
