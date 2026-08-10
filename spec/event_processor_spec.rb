# frozen_string_literal: true

require "spec_helper"
require "socket"
require "timeout"

RSpec.describe FeatBit::EventProcessor do
  it "batches and flushes events on its worker" do
    batches = Queue.new
    options = FeatBit::Options.new(env_secret: "secret", events_flush_interval: 60, events_batch_size: 2)
    processor = described_class.new(options, sender: ->(batch) { batches << batch })
    processor.enqueue(id: 1)
    processor.enqueue(id: 2)
    expect(batches.pop.map { |event| event[:id] }).to eq([1, 2])
    expect(processor.close).to be(true)
  end

  it "drops instead of blocking when the bounded queue is full" do
    options = FeatBit::Options.new(env_secret: "secret", events_capacity: 1, events_flush_interval: 60)
    processor = described_class.allocate
    processor.instance_variable_set(:@options, options)
    processor.instance_variable_set(:@queue, SizedQueue.new(1))
    processor.instance_variable_set(:@control_queue, Queue.new)
    processor.instance_variable_set(:@dropped_events, 0)
    processor.instance_variable_set(:@drop_mutex, Mutex.new)
    processor.instance_variable_set(:@state_mutex, Mutex.new)
    processor.instance_variable_set(:@close_mutex, Mutex.new)
    processor.instance_variable_set(:@closed, false)
    processor.instance_variable_set(:@close_result, nil)
    expect(processor.enqueue(id: 1)).to be(true)
    expect(processor.enqueue(id: 2)).to be(false)
    expect(processor.dropped_events).to eq(1)
  end

  it "posts FeatBit events with authentication and JSON payload" do
    server = TCPServer.new("127.0.0.1", 0)
    received = Queue.new
    server_thread = Thread.new do
      socket = server.accept
      request_line = socket.gets
      headers = {}
      while (line = socket.gets) && line != "\r\n"
        key, value = line.split(":", 2)
        headers[key.downcase] = value.strip
      end
      body = socket.read(headers.fetch("content-length").to_i)
      received << [request_line, headers, body]
      socket.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
      socket.close
    end

    options = FeatBit::Options.new(
      env_secret: "secret",
      event_url: "http://127.0.0.1:#{server.local_address.ip_port}",
      events_batch_size: 1
    )
    processor = described_class.new(options)
    expect(processor.enqueue(id: 1)).to be(true)
    request_line, headers, body = Timeout.timeout(3) { received.pop }
    expect(request_line).to start_with("POST /api/public/insight/track")
    expect(headers["authorization"]).to eq("secret")
    expect(JSON.parse(body)).to eq([{ "id" => 1 }])
    expect(processor.close).to be(true)
  ensure
    server&.close
    server_thread&.join(1)
  end

  it "reports delivery failure from an explicit flush" do
    attempts = 0
    options = FeatBit::Options.new(env_secret: "secret", events_flush_interval: 60)
    processor = described_class.new(options, sender: lambda { |_batch|
      attempts += 1
      false
    })
    expect(processor.enqueue(id: 1)).to be(true)
    expect(processor.flush).to be(false)
    expect(attempts).to eq(3)
    expect(processor.close).to be(true)
  end

  it "attempts later slices when an earlier slice is rejected" do
    delivered_ids = []
    options = FeatBit::Options.new(env_secret: "secret", events_flush_interval: 60, events_batch_size: 1)
    processor = described_class.new(options, sender: lambda { |batch|
      id = batch.fetch(0).fetch(:id)
      delivered_ids << id
      raise FeatBit::EventProcessor::DeliveryRejected if id == 1

      true
    })
    processor.enqueue(id: 1)
    processor.enqueue(id: 2)

    expect(processor.flush).to be(false)
    expect(delivered_ids).to eq([1, 2])
    expect(processor.close).to be(true)
  end

  it "closes immediately when its worker could not start" do
    allow(Thread).to receive(:new).and_raise("thread unavailable")
    processor = described_class.new(FeatBit::Options.new(env_secret: "secret"))
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    expect(processor.close).to be(true)
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 0.1
  end

  it "partitions events drained by flush to the configured server batch size" do
    batches = []
    options = FeatBit::Options.new(env_secret: "secret", events_flush_interval: 60, events_batch_size: 2)
    processor = described_class.new(options, sender: lambda { |batch|
      batches << batch
      true
    })
    5.times { |id| processor.enqueue(id: id) }

    expect(processor.flush).to be(true)
    expect(batches.flatten.map { |event| event[:id] }).to contain_exactly(0, 1, 2, 3, 4)
    expect(batches.map(&:length)).to all(be <= 2)
    expect(processor.close).to be(true)
  end

  it "closes without leaking a worker when the event queue is full" do
    first_delivery = Queue.new
    release = Queue.new
    calls = 0
    sender = lambda do |_batch|
      calls += 1
      if calls == 1
        first_delivery << true
        release.pop
      end
      true
    end
    options = FeatBit::Options.new(env_secret: "secret", events_capacity: 1, events_batch_size: 1)
    processor = described_class.new(options, sender: sender)
    expect(processor.enqueue(id: 1)).to be(true)
    Timeout.timeout(2) { first_delivery.pop }
    expect(processor.enqueue(id: 2)).to be(true)
    close_thread = Thread.new { processor.close }
    release << true

    expect(Timeout.timeout(2) { close_thread.value }).to be(true)
    expect(calls).to eq(2)
  end

  it "makes concurrent close idempotent" do
    options = FeatBit::Options.new(env_secret: "secret")
    processor = described_class.new(options, sender: ->(_batch) { true })
    results = 10.times.map { Thread.new { processor.close } }.map(&:value)

    expect(results).to all(be(true))
  end

  it "orders a concurrent flush before shutdown once flush is accepted" do
    options = FeatBit::Options.new(env_secret: "secret", events_flush_interval: 60)
    processor = described_class.new(options, sender: ->(_batch) { true })
    original_queue = processor.instance_variable_get(:@control_queue)
    flush_started = Queue.new
    release_flush = Queue.new
    controlled_queue = Object.new
    controlled_queue.define_singleton_method(:<<) do |message|
      if message[:type] == FeatBit::EventProcessor::FLUSH
        flush_started << true
        release_flush.pop
      end
      original_queue << message
    end
    controlled_queue.define_singleton_method(:pop) { |*args| original_queue.pop(*args) }
    processor.instance_variable_set(:@control_queue, controlled_queue)
    processor.enqueue(id: 1)

    flush_thread = Thread.new { processor.flush }
    Timeout.timeout(2) { flush_started.pop }
    close_thread = Thread.new { processor.close }
    release_flush << true

    expect(Timeout.timeout(2) { flush_thread.value }).to be(true)
    expect(Timeout.timeout(2) { close_thread.value }).to be(true)
  end

  it "does not retain worker threads across repeated lifecycles" do
    existing_ids = Thread.list.filter_map { |thread| thread.object_id if thread.name == "featbit-event-processor" }
    options = FeatBit::Options.new(env_secret: "secret")
    20.times do
      processor = described_class.new(options, sender: ->(_batch) { true })
      expect(processor.close).to be(true)
    end

    leaked = Thread.list.select do |thread|
      thread.name == "featbit-event-processor" && !existing_ids.include?(thread.object_id) && thread.alive?
    end
    expect(leaked).to be_empty
  end
end
