# frozen_string_literal: true

Rails.application.routes.draw do
  get "/flags/:key", to: "flags#show"
end
