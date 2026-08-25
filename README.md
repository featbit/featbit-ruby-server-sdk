# FeatBit Server-Side SDK for Ruby

## Introduction

This is the Ruby Server-Side SDK for the 100% open-source feature flag management platform [FeatBit](https://github.com/featbit/featbit).

The SDK is designed for multi-user server applications such as Rails services, background workers, and command-line applications. Create one `FeatBit::Client` per application process and reuse it for the lifetime of that process.

## Data Synchronization

The SDK maintains a WebSocket connection to the FeatBit evaluation service. Feature flags and segments are synchronized into a thread-safe in-memory store, so flag evaluation is local and does not make a network request.

Changes are pushed to the SDK as full snapshots or incremental patches. Interrupted connections are retried automatically with capped exponential backoff, and the SDK sends periodic heartbeat messages. 

## Get Started

### Installation

Add the SDK to your `Gemfile`:

```ruby
gem "featbit-server-sdk"
```

Then run:

```sh
bundle install
```

### Prerequisite

Before using the SDK, obtain the environment secret and SDK URLs from your FeatBit environment:

- [How to get the environment secret](https://docs.featbit.co/sdk/faq#how-to-get-the-environment-secret)
- [How to get the SDK URLs](https://docs.featbit.co/sdk/faq#how-to-get-the-sdk-urls)

Keep the environment secret on the server. Do not expose it to browsers or commit it to source control.

### Quick Start

```ruby
require "featbit"

options = FeatBit::Options.new(
  env_secret: ENV.fetch("FEATBIT_ENV_SECRET"),
  streaming_url: ENV.fetch("FEATBIT_STREAMING_URL", "wss://app-eval.featbit.co"),
  event_url: ENV.fetch("FEATBIT_EVENT_URL", "https://app-eval.featbit.co"),
  start_wait: 5
)

client = FeatBit::Client.new(options)

unless client.initialized?
  warn "FeatBit failed to initialize; variation calls will return fallback values"
end

flag_key = "game-runner"
user = FeatBit::User.new("anonymous", name: "Anonymous", custom: { country: "FR" })

value = client.bool_variation(flag_key, user, false)
detail = client.variation_detail(flag_key, user, false)

puts "flag=#{flag_key} value=#{value} variation_id=#{detail.variation_id} reason=#{detail.reason}"

# Close the client during application shutdown so queued insight events are flushed.
client.close
```

### Examples

- [Console](./examples/Console)
- [Rails](./examples/Rails)

## SDK

### `FeatBit::Client`

`FeatBit::Client` owns the WebSocket synchronizer, in-memory flag store, evaluator, and event processor. Applications should instantiate a single client for each process and close it during graceful shutdown.

#### Default FeatBit Cloud endpoints

```ruby
client = FeatBit::Client.new(
  FeatBit::Options.new(env_secret: ENV.fetch("FEATBIT_ENV_SECRET"))
)
```

#### Custom endpoints and logger

```ruby
require "logger"

options = FeatBit::Options.new(
  env_secret: ENV.fetch("FEATBIT_ENV_SECRET"),
  streaming_url: "ws://localhost:5100",
  event_url: "http://localhost:5100",
  start_wait: 3,
  connect_timeout: 5,
  read_timeout: 10,
  logger: Logger.new($stdout, level: Logger::INFO)
)

client = FeatBit::Client.new(options)
```

#### Status

The status provider exposes the synchronization lifecycle without throwing into application code:

```ruby
client.status_provider.status
client.status_provider.ready?
client.status_provider.wait_until_ready(5)

listener_id = client.status_provider.add_listener do |status, message|
  puts "FeatBit status=#{status} message=#{message}"
end

client.status_provider.remove_listener(listener_id)
```

Possible states are `starting`, `ready`, `interrupted`, `offline`, `failed`, and `closed`.

#### Logging

Pass any logger compatible with Ruby's standard `Logger` interface through `FeatBit::Options`. Transport, evaluation, listener, and event delivery failures are logged without including the environment secret.

### `FeatBit::User`

A user has a required unique key, an optional name, and optional custom attributes. Built-in and custom attributes can be referenced by targeting rules and are included in analytics events.

```ruby
user = FeatBit::User.new(
  "a-unique-user-key",
  name: "Bob",
  custom: { age: 15, country: "FR", plan: "pro" }
)
```

User attributes are defensively copied and frozen so the same user can safely be shared by concurrent evaluations.

### Evaluating flags

Evaluation is synchronous and local. Every call accepts a flag key, user, and fallback value. If the client is not ready, the flag is missing, the value has the wrong type, or an internal error occurs, the fallback is returned instead of raising into application code.

```ruby
enabled = client.bool_variation("game-runner", user, false)
title = client.string_variation("page-title", user, "Welcome")
price = client.number_variation("price", user, 0)
config = client.json_variation("checkout-config", user, {})

detail = client.variation_detail("game-runner", user, false)
puts detail.value
puts detail.variation_id
puts detail.reason
puts detail.error_kind
puts detail.error_message
```

Available APIs:

- `variation` and `variation_detail`
- `bool_variation`
- `string_variation`
- `number_variation`
- `json_variation`

### Flag change notifications

```ruby
listener_id = client.add_flag_change_listener do |flag_key|
  puts "configuration changed for #{flag_key}"
end

client.remove_flag_change_listener(listener_id)
```

Callbacks run outside internal locks. Exceptions raised by application callbacks are logged and isolated.

### Offline Mode

Offline mode disables WebSocket synchronization and event delivery. Supply a full FeatBit data-sync payload to evaluate locally:

```ruby
require "json"

bootstrap = JSON.parse(File.read("featbit-bootstrap.json"))
options = FeatBit::Options.new(offline: true, bootstrap: bootstrap)
client = FeatBit::Client.new(options)
```

An existing snapshot can be downloaded from the evaluation service:

```sh
curl -H "Authorization: <your-env-secret>" \
  https://app-eval.featbit.co/api/public/sdk/server/latest-all \
  > featbit-bootstrap.json
```

The bootstrap format is owned by FeatBit and may evolve; prefer downloading an actual snapshot rather than constructing one manually.

### Disable Events Collection

Online synchronization can remain enabled while analytics events are disabled:

```ruby
options = FeatBit::Options.new(
  env_secret: ENV.fetch("FEATBIT_ENV_SECRET"),
  disable_events: true
)
```

### Experiments (A/B/n Testing)

Successful flag evaluations automatically enqueue variation events. Custom metric events can be sent with `track`:

```ruby
client.bool_variation("new-checkout", user, false)
client.track(user, "checkout_completed", 99.0)
client.flush
```

Call `track` after evaluating the related experiment flag. `numeric_value` defaults to `1.0`.

### Thread safety and shutdown

- Share one client across application threads; do not create a client per request.
- Flag evaluation performs no network I/O.
- Event enqueue is non-blocking and bounded; overload increments `dropped_events`.
- Status and flag callbacks execute outside internal locks.
- `close` is thread-safe and idempotent, flushes accepted events, stops background workers, and prevents new events.
- Public client methods contain ordinary internal failures and return fallbacks or `false`.

## Supported Ruby versions

- Ruby 3.1 and later
- MRI Ruby is covered by the CI test matrix

## OpenFeature

Use the separate [FeatBit OpenFeature Ruby provider](https://github.com/featbit/openfeature-provider-ruby-server) to access this SDK through the standard OpenFeature API.

## Development and review checks

```sh
bundle install
bundle exec rspec
bundle exec rubocop
bundle exec ruby scripts/resource_audit.rb
gem build featbit-server-sdk.gemspec
```

The test suite covers evaluation precedence, typed values, segments and cyclic segment protection, protocol payloads, full/patch synchronization, equal-timestamp updates, concurrent data access, bounded event queues, concurrent/idempotent shutdown, thread cleanup, listener mutation, and exception isolation.

See [docs/REVIEW.md](./docs/REVIEW.md) for the code-review findings, fixes, and verification matrix.

For an opt-in live check against FeatBit Cloud:

```sh
FEATBIT_ENV_SECRET="..." \
FEATBIT_FLAG_KEY="game-runner" \
bundle exec ruby scripts/live_integration_check.rb
```

## Getting support

- Ask questions in [FeatBit Slack](https://join.slack.com/t/featbit/shared_invite/zt-1ew5e2vbb-x6Apan1xZOaYMnFzqZkGNQ).
- Report bugs or request features in [GitHub Issues](https://github.com/featbit/featbit-ruby-server-sdk/issues/new).

## See Also

- [FeatBit documentation](https://docs.featbit.co)
- [Connect an SDK](https://docs.featbit.co/getting-started/connect-an-sdk)
