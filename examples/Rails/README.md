# Rails example

This minimal Rails application exposes `GET /flags/:key?user_key=...` and returns FeatBit evaluation details as JSON.

The `FeatBitClientRegistry` creates the client lazily on the first request. This keeps background threads out of a preloaded master process and guarantees one client per Rails worker process. `at_exit` closes the client during graceful shutdown.

```sh
cd examples/Rails
bundle install

FEATBIT_ENV_SECRET="..." bundle exec rails server
```

Then request:

```sh
curl "http://localhost:3000/flags/game-runner?user_key=rails-user"
```

For Puma cluster mode, keep the registry lazy so each worker creates its own WebSocket connection after fork.
