# frozen_string_literal: true

module FeatBit
  class User
    attr_reader :key, :name, :custom

    def initialize(key, name: nil, custom: {})
      @key = key.to_s
      @name = name
      @custom = deep_freeze(deep_copy(custom || {}).transform_keys(&:to_s))
      freeze
    rescue StandardError
      @key = ""
      @name = nil
      @custom = {}.freeze
      freeze
    end

    def valid?
      !key.empty?
    end

    def [](attribute)
      normalized = attribute.to_s.downcase
      return key if %w[key keyid targeting_key].include?(normalized)
      return name if normalized == "name"

      custom[attribute.to_s] || custom[normalized]
    rescue StandardError
      nil
    end

    def to_h
      {
        "keyId" => key,
        "name" => name,
        "customizedProperties" => custom.map do |property_name, value|
          { "name" => property_name, "value" => value.to_s }
        end
      }
    end

    private

    def deep_copy(value)
      case value
      when Hash then value.each_with_object({}) { |(key, item), copy| copy[key.to_s] = deep_copy(item) }
      when Array then value.map { |item| deep_copy(item) }
      when String then value.dup
      else value
      end
    end

    def deep_freeze(value)
      case value
      when Hash
        value.each do |key, item|
          deep_freeze(key)
          deep_freeze(item)
        end
      when Array then value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end
  end
end
