# frozen_string_literal: true

class FlagsController < ApplicationController
  def show
    user = FeatBit::User.new(
      params.fetch(:user_key, "rails-user"),
      custom: { application: "rails", language: "ruby" }
    )
    detail = featbit_client.variation_detail(params[:key], user, false)

    render json: {
      key: params[:key],
      value: detail.value,
      variation_id: detail.variation_id,
      reason: detail.reason,
      error_kind: detail.error_kind,
      error_message: detail.error_message
    }.compact
  end

  private

  def featbit_client
    Rails.application.config.x.featbit.client
  end
end
