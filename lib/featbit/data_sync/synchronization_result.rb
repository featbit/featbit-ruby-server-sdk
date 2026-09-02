# frozen_string_literal: true

module FeatBit
  class SynchronizationResult
    attr_reader :valid, :changed

    def initialize(valid:, changed:)
      @valid = valid
      @changed = changed
      freeze
    end

    def valid? = valid
    def changed? = changed

    INVALID = new(valid: false, changed: false)
    UNCHANGED = new(valid: true, changed: false)
    CHANGED = new(valid: true, changed: true)

    def self.valid(changed:)
      changed ? CHANGED : UNCHANGED
    end
  end
end
