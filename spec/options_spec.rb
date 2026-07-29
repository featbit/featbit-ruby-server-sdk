# frozen_string_literal: true

require "spec_helper"

RSpec.describe FeatBit::Options do
  it "builds FeatBit protocol endpoints" do
    options = described_class.new(env_secret: "secret", streaming_url: "wss://example.test/", event_url: "https://example.test/")
    expect(options.streaming_uri).to eq("wss://example.test/streaming")
    expect(options.events_uri).to eq("https://example.test/api/public/insight/track")
    expect(options).to be_valid
  end

  it "does not raise for invalid user configuration" do
    expect { described_class.new(start_wait: Object.new) }.not_to raise_error
  end
end
