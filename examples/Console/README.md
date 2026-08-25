# Console example

This example connects to FeatBit, evaluates a flag locally, sends a custom event, flushes, and closes the SDK.

```sh
cd examples/Console
bundle install

FEATBIT_ENV_SECRET="..." \
FEATBIT_FLAG_KEY="game-runner" \
bundle exec ruby app.rb
```

Optional variables:

- `FEATBIT_STREAMING_URL` defaults to `wss://app-eval.featbit.co`
- `FEATBIT_EVENT_URL` defaults to `https://app-eval.featbit.co`
- `FEATBIT_USER_KEY` defaults to `console-user`
