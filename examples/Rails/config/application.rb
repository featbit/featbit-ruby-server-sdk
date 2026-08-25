# frozen_string_literal: true

require_relative "boot"
require "rails"
require "action_controller/railtie"

Bundler.require(*Rails.groups)

module FeatBitRailsExample
  class Application < Rails::Application
    config.load_defaults 7.2
    config.autoload_paths << Rails.root.join("lib")
    config.api_only = true
  end
end
