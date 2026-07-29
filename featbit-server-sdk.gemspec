# frozen_string_literal: true

require_relative "lib/featbit/version"

Gem::Specification.new do |spec|
  spec.name = "featbit-server-sdk"
  spec.version = FeatBit::VERSION
  spec.authors = ["FeatBit"]
  spec.email = ["contact@featbit.co"]
  spec.summary = "FeatBit Server-Side SDK for Ruby"
  spec.description = "Thread-safe Ruby SDK for evaluating FeatBit feature flags locally."
  spec.homepage = "https://github.com/featbit/featbit-ruby-server-sdk"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "#{spec.homepage}#readme"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.files = Dir.glob("{lib}/**/*") + %w[README.md LICENSE CHANGELOG.md]
  spec.require_paths = ["lib"]
  spec.add_dependency "logger", "~> 1.6"
  spec.add_dependency "websocket-client-simple", "~> 0.9"
end
