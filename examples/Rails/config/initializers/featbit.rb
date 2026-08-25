# frozen_string_literal: true

require "featbit"
require "featbit_client_registry"

registry = FeatBitClientRegistry.new(env: ENV, logger: Rails.logger)
Rails.application.config.x.featbit = registry

at_exit { registry.close }
