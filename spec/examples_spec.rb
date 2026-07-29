# frozen_string_literal: true

require "spec_helper"
require_relative "../examples/Rails/lib/featbit_client_registry"

RSpec.describe "repository examples" do
  it "keeps the Rails client process-scoped, lazy, and thread-safe" do
    logger = Logger.new(nil)
    fake_client = instance_double(FeatBit::Client, close: true)
    creations = Queue.new
    registry = FeatBitClientRegistry.new(
      env: { "FEATBIT_ENV_SECRET" => "secret" },
      logger: logger,
      client_factory: lambda { |_options|
        creations << true
        fake_client
      }
    )

    clients = 20.times.map { Thread.new { registry.client } }.map(&:value)

    expect(clients).to all(be(fake_client))
    expect(creations.size).to eq(1)
    expect(registry.close).to be(true)
    expect(registry.close).to be(true)
    expect(fake_client).to have_received(:close).once
    expect { registry.client }.to raise_error("FeatBit client registry is closed")
  end

  it "keeps every committed example and audit script syntactically valid" do
    root = File.expand_path("..", __dir__)
    files = Dir.glob(File.join(root, "{examples,scripts}", "**", "*.rb"))

    expect(files).not_to be_empty
    files.each do |file|
      expect(RubyVM::InstructionSequence.compile_file(file)).to be_a(RubyVM::InstructionSequence)
    end
  end
end
