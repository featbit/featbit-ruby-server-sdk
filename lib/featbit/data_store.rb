# frozen_string_literal: true

require "json"
require "time"

module FeatBit
  class InMemoryDataStore
    def initialize
      @flags = {}.freeze
      @segments = {}.freeze
      @flag_versions = {}.freeze
      @segment_versions = {}.freeze
      @version = 0
      @initialized = false
      @mutex = Mutex.new
    end

    def initialized?
      @mutex.synchronize { @initialized }
    rescue StandardError
      false
    end

    def version
      @mutex.synchronize { @version }
    rescue StandardError
      0
    end

    def flag(key)
      item = @mutex.synchronize { @flags[key.to_s] }
      item && !fetch(item, "isArchived", false) ? deep_dup(item) : nil
    rescue StandardError
      nil
    end

    def segment(id)
      item = @mutex.synchronize { @segments[id.to_s] }
      item && !fetch(item, "isArchived", false) ? deep_dup(item) : nil
    rescue StandardError
      nil
    end

    def all_flags
      snapshot = @mutex.synchronize { @flags }
      snapshot.each_with_object({}) do |(key, value), result|
        result[key] = deep_dup(value) unless fetch(value, "isArchived", false)
      end
    rescue StandardError
      {}
    end

    def init(payload, version: nil)
      data = normalize_payload(payload)
      flags = Array(fetch(data, "featureFlags", [])).to_h do |flag|
        [fetch(flag, "key").to_s, normalize_flag(flag)]
      end
      segments = Array(fetch(data, "segments", [])).to_h do |segment|
        [fetch(segment, "id").to_s, normalize_hash(segment)]
      end
      incoming_version = version || derive_version(flags.values + segments.values)
      incoming_version = 1 if incoming_version.to_i <= 0
      flag_versions = entity_versions(flags, version)
      segment_versions = entity_versions(segments, version)

      @mutex.synchronize do
        return false if @initialized && incoming_version.to_i <= @version

        @flags = deep_freeze(flags)
        @segments = deep_freeze(segments)
        @flag_versions = deep_freeze(flag_versions)
        @segment_versions = deep_freeze(segment_versions)
        @version = incoming_version.to_i
        @initialized = true
      end
      true
    rescue StandardError
      false
    end

    def upsert(kind, item, version: nil)
      normalized = normalize_hash(item)
      key = kind.to_sym == :flags ? fetch(normalized, "key") : fetch(normalized, "id")
      return false if key.to_s.empty?

      incoming_version = version || item_version(normalized)
      incoming_version = @mutex.synchronize { @version + 1 } if incoming_version.to_i <= 0
      @mutex.synchronize do
        if kind.to_sym == :flags
          return false if incoming_version.to_i <= @flag_versions.fetch(key.to_s, 0)

          copy = @flags.dup
          version_copy = @flag_versions.dup
          copy[key.to_s] = normalize_flag(normalized)
          version_copy[key.to_s] = incoming_version.to_i
          @flags = deep_freeze(copy)
          @flag_versions = deep_freeze(version_copy)
        else
          return false if incoming_version.to_i <= @segment_versions.fetch(key.to_s, 0)

          copy = @segments.dup
          version_copy = @segment_versions.dup
          copy[key.to_s] = normalized
          version_copy[key.to_s] = incoming_version.to_i
          @segments = deep_freeze(copy)
          @segment_versions = deep_freeze(version_copy)
        end
        @version = [@version, incoming_version.to_i].max
        @initialized = true
      end
      true
    rescue StandardError
      false
    end

    private

    def normalize_payload(payload)
      parsed = payload.is_a?(String) ? JSON.parse(payload) : payload
      parsed = fetch(parsed, "data", parsed)
      normalize_hash(parsed || {})
    end

    def normalize_flag(flag)
      normalized = normalize_hash(flag)
      variations = Array(fetch(normalized, "variations", []))
      normalized["variationMap"] = variations.to_h do |variation|
        [fetch(variation, "id").to_s, fetch(variation, "value")]
      end
      normalized
    end

    def normalize_hash(value)
      case value
      when Hash
        value.each_with_object({}) { |(key, item), result| result[key.to_s] = normalize_hash(item) }
      when Array
        value.map { |item| normalize_hash(item) }
      else
        value
      end
    end

    def derive_version(items)
      items.map { |item| item_version(item) }.max.to_i
    end

    def entity_versions(items, snapshot_version)
      items.transform_values do |item|
        next snapshot_version.to_i if snapshot_version

        version = item_version(item)
        version.positive? ? version : 1
      end
    end

    def item_version(item)
      explicit = fetch(item, "timestamp")
      return explicit.to_i if explicit

      updated = fetch(item, "updatedAt")
      updated ? (Time.parse(updated.to_s).to_f * 1000).to_i : 0
    rescue StandardError
      0
    end

    def fetch(hash, key, default = nil)
      hash.is_a?(Hash) ? hash.fetch(key.to_s, hash.fetch(key.to_sym, default)) : default
    end

    def deep_dup(value)
      Marshal.load(Marshal.dump(value))
    end

    def deep_freeze(value)
      case value
      when Hash
        value.each do |key, item|
          deep_freeze(key)
          deep_freeze(item)
        end
      when Array
        value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end
  end
end
