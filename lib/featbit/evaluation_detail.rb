# frozen_string_literal: true

module FeatBit
  EvaluationDetail = Struct.new(
    :value, :reason, :variation_id, :flag_key, :flag_name, :send_to_experiment, :error_kind, :error_message,
    keyword_init: true
  ) do
    def success?
      error_kind.nil?
    end
  end
end
