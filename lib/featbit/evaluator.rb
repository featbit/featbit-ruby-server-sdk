# frozen_string_literal: true

require "digest/md5"
require "json"
require "set"

module FeatBit
  class Evaluator
    REGEXP_TIMEOUT = 0.05
    REGEXP_TIMEOUT_SUPPORTED = begin
      Regexp.new("", timeout: REGEXP_TIMEOUT)
      true
    rescue ArgumentError, TypeError
      false
    end

    REASONS = {
      client_not_ready: "client not ready",
      flag_not_found: "flag not found",
      error: "error in evaluation",
      user_not_specified: "user not specified",
      wrong_type: "wrong type",
      flag_off: "flag off",
      target_match: "target match",
      rule_match: "rule match",
      fallthrough: "fall through all rules"
    }.freeze

    def initialize(flag_getter:, segment_getter:, logger: nil)
      @flag_getter = flag_getter
      @segment_getter = segment_getter
      @logger = logger
    end

    def evaluate(flag_key, user, default_value)
      return error_detail(flag_key, default_value, :user_not_specified) unless user&.valid?

      flag = @flag_getter.call(flag_key.to_s)
      return error_detail(flag_key, default_value, :flag_not_found) unless flag

      variation_id, reason, send_to_experiment = select_variation(flag, user, Set.new)
      raise "no variation matched" if variation_id.to_s.empty?

      raw_value = fetch(fetch(flag, "variationMap", {}), variation_id)
      raise "variation value not found" if raw_value.nil?

      value = cast(raw_value, fetch(flag, "variationType"))
      EvaluationDetail.new(
        value: value,
        reason: REASONS.fetch(reason),
        variation_id: variation_id,
        flag_key: fetch(flag, "key", flag_key),
        flag_name: fetch(flag, "name"),
        send_to_experiment: send_to_experiment
      )
    rescue StandardError => e
      @logger&.error("FeatBit evaluation failed: #{e.message}")
      error_detail(flag_key, default_value, :error, e.message)
    end

    private

    def select_variation(flag, user, visited_segments)
      return [fetch(flag, "disabledVariationId"), :flag_off, false] unless fetch(flag, "isEnabled", false)

      Array(fetch(flag, "targetUsers", [])).each do |target|
        if Array(fetch(target, "keyIds", [])).map(&:to_s).include?(user.key)
          return [fetch(target, "variationId"), :target_match, fetch(flag, "exptIncludeAllTargets", false) == true]
        end
      end

      Array(fetch(flag, "rules", [])).each do |rule|
        next unless Array(fetch(rule, "conditions", [])).all? do |condition|
          condition_matches?(user, condition, visited_segments)
        end

        variation_id, send_to_experiment = rollout_variation(flag, rule, user)
        return [variation_id, :rule_match, send_to_experiment] if variation_id
      end

      variation_id, send_to_experiment = rollout_variation(flag, fetch(flag, "fallthrough", {}), user)
      [variation_id, :fallthrough, send_to_experiment]
    end

    def rollout_variation(flag, rollout, user)
      dispatch_key = fetch(rollout, "dispatchKey") || "keyId"
      dispatch_value = user[dispatch_key].to_s
      bucket_key = "#{fetch(flag, 'key')}#{dispatch_value}"
      Array(fetch(rollout, "variations", [])).each do |variation|
        range = Array(fetch(variation, "rollout", []))
        next unless in_bucket?(bucket_key, range)

        return [fetch(variation, "id"), send_to_experiment?(flag, rollout, variation, bucket_key, range)]
      end
      [nil, false]
    end

    def send_to_experiment?(flag, rollout, variation, bucket_key, range)
      return true if fetch(flag, "exptIncludeAllTargets", false)
      return false unless fetch(rollout, "includedInExpt", false)

      span = range[1].to_f - range[0].to_f
      experiment_rollout = fetch(variation, "exptRollout", 0).to_f
      return false unless span.positive? && experiment_rollout.positive?

      upper_bound = [experiment_rollout / span, 1.0].min
      in_bucket?("expt#{bucket_key}", [0, upper_bound])
    rescue StandardError
      false
    end

    def in_bucket?(key, range)
      return false unless range.length == 2
      return true if range[0].to_f.zero? && range[1].to_f >= 1.0

      signed = Digest::MD5.digest(key.to_s).byteslice(0, 4).unpack1("l<")
      percentage = (signed / -2_147_483_648.0).abs
      percentage >= range[0].to_f && percentage < range[1].to_f
    rescue StandardError
      false
    end

    def condition_matches?(user, condition, visited_segments = Set.new)
      property = fetch(condition, "property")
      operation = fetch(condition, "op") || property
      actual = user[property]
      expected = fetch(condition, "value")
      actual_number = numeric_value(actual)
      expected_number = numeric_value(expected)

      case operation
      when "BiggerEqualThan" then actual_number && expected_number && actual_number >= expected_number
      when "BiggerThan" then actual_number && expected_number && actual_number > expected_number
      when "LessEqualThan" then actual_number && expected_number && actual_number <= expected_number
      when "LessThan" then actual_number && expected_number && actual_number < expected_number
      when "Equal" then !actual.nil? && !expected.nil? && actual.to_s == expected.to_s
      when "NotEqual" then actual.nil? || expected.nil? || actual.to_s != expected.to_s
      when "Contains" then !actual.nil? && !expected.nil? && actual.to_s.include?(expected.to_s)
      when "NotContain" then actual.nil? || expected.nil? || !actual.to_s.include?(expected.to_s)
      when "IsOneOf" then json_array(expected).map(&:to_s).include?(actual.to_s)
      when "NotOneOf" then !json_array(expected).map(&:to_s).include?(actual.to_s)
      when "StartsWith" then !actual.nil? && actual.to_s.start_with?(expected.to_s)
      when "EndsWith" then !actual.nil? && actual.to_s.end_with?(expected.to_s)
      when "IsTrue" then actual == true || actual.to_s.casecmp("true").zero?
      when "IsFalse" then actual == false || actual.to_s.casecmp("false").zero?
      when "MatchRegex" then !actual.nil? && regex_match(expected, actual) == true
      when "NotMatchRegex" then actual.nil? || regex_match(expected, actual) == false
      when "User is in segment" then segment_match?(user, expected, visited_segments)
      when "User is not in segment" then !segment_match?(user, expected, visited_segments)
      else false
      end
    rescue StandardError
      false
    end

    def regex_match(pattern, value)
      regexp = if REGEXP_TIMEOUT_SUPPORTED
                 Regexp.new(pattern.to_s, timeout: REGEXP_TIMEOUT)
               else
                 Regexp.new(pattern.to_s)
               end
      regexp.match?(value.to_s)
    rescue StandardError
      nil
    end

    def segment_match?(user, serialized_ids, visited_segments)
      json_array(serialized_ids).any? do |segment_id|
        normalized_id = segment_id.to_s
        next false if visited_segments.include?(normalized_id)

        segment = @segment_getter.call(normalized_id)
        next false unless segment
        next false if Array(fetch(segment, "excluded", [])).map(&:to_s).include?(user.key)
        next true if Array(fetch(segment, "included", [])).map(&:to_s).include?(user.key)

        nested_visited = visited_segments | [normalized_id]
        Array(fetch(segment, "rules", [])).any? do |rule|
          Array(fetch(rule, "conditions", [])).all? do |condition|
            condition_matches?(user, condition, nested_visited)
          end
        end
      end
    end

    def cast(value, type)
      case type.to_s.downcase
      when "boolean" then cast_boolean(value)
      when "number" then numeric_cast(value)
      when "json" then value.is_a?(String) ? JSON.parse(value) : value
      else value.to_s
      end
    end

    def cast_boolean(value)
      return value if [true, false].include?(value)
      return true if value.is_a?(String) && value.casecmp("true").zero?
      return false if value.is_a?(String) && value.casecmp("false").zero?

      raise "invalid boolean variation"
    end

    def numeric_cast(value)
      number = Float(value)
      number.finite? && number == number.to_i ? number.to_i : number
    end

    def numeric_value(value)
      Float(value).round(5)
    rescue StandardError
      nil
    end

    def json_array(value)
      parsed = value.is_a?(String) ? JSON.parse(value) : value
      parsed.is_a?(Array) ? parsed : []
    rescue StandardError
      []
    end

    def error_detail(flag_key, default_value, kind, message = nil)
      EvaluationDetail.new(
        value: default_value,
        reason: REASONS.fetch(kind),
        flag_key: flag_key,
        error_kind: kind,
        error_message: message
      )
    end

    def fetch(hash, key, default = nil)
      hash.is_a?(Hash) ? hash.fetch(key.to_s, hash.fetch(key.to_sym, default)) : default
    end
  end
end
