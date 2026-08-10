# frozen_string_literal: true

require "spec_helper"

RSpec.describe FeatBit::StatusProvider do
  it "does not expose listener or logger failures" do
    logger = Object.new
    logger.define_singleton_method(:warn) { |_message| raise "logger failed" }
    provider = described_class.new(logger: logger)
    provider.add_listener { raise "listener failed" }

    expect { provider.update(FeatBit::Status::READY) }.not_to raise_error
    expect(provider.status).to eq(FeatBit::Status::READY)
  end
end
