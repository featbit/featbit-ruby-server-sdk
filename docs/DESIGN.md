# FeatBit Ruby Server SDK design

## Scope

The SDK owns FeatBit transport, storage, evaluation, event delivery, logging, lifecycle, concurrency, and safe application-facing APIs. OpenFeature-specific types remain in the separate Provider gem.

## Components

### 0. `FeatBit::Options`

`Options` is an immutable configuration value containing the environment secret, streaming/event endpoints, startup wait, offline bootstrap, event queue limits, timeouts, retry delay, logger, and injectable factories for testing. Derived endpoints are `/streaming` and `/api/public/insight/track`. Invalid values are normalized to safe defaults and `valid?` reports whether online operation is possible.

### 1. `WebSocketDataSynchronizer`

The synchronizer authenticates with the FeatBit token format and the environment secret header. On open it sends a `data-sync` request containing the local timestamp. It accepts `full` and `patch` envelopes, updates the data store, broadcasts affected flag keys, reports lifecycle states, and reconnects with capped exponential delay. Parsing and callback errors are contained inside the component.

### 2. `EventProcessor`

The event processor uses a bounded `SizedQueue` and one worker thread. Producers use non-blocking enqueue; a full queue increments `dropped_events` rather than delaying business requests. It flushes on batch size, interval, explicit `flush`, and `close`, posting JSON to the FeatBit insight endpoint. Offline and disabled-event configurations use `NullEventProcessor`.

### 3. `Evaluator`

Evaluation is local and synchronous. Precedence is: disabled variation, explicit target, first matching rule, then fallthrough. Conditions cover numeric comparison, equality, containment, lists, prefixes/suffixes, booleans, regular expressions, and segment membership. Percentage rollout uses FeatBit's MD5/little-endian bucketing algorithm. Results include typed value, reason, variation ID, flag key/name, and structured errors.

### 4. Logging

Ruby's standard `Logger` is accepted through `Options`. Internal transport, listener, event, and evaluation failures are logged without exposing secrets or raising into user code.

### 5. Thread safety, performance, and exception boundary

- Data snapshots are replaced under a mutex and then read as frozen structures; callers receive defensive copies.
- Status and flag listeners are copied under lock and invoked outside the lock, avoiding deadlocks and allowing listener mutation during callbacks.
- Evaluation does no network I/O.
- Event enqueue is bounded and non-blocking.
- Public client evaluation methods return defaults/details on failure; lifecycle and event APIs return booleans.
- `close` is idempotent.

## Verification

The suite covers offline evaluation, targets, rules, segments, typed JSON, variation IDs, immutable storage, version ordering, concurrent readers/writers, bounded events, WebSocket data parsing, listener concurrency, and public exception isolation. The opt-in live check additionally verifies authentication, WebSocket full synchronization, local evaluation of remote data, event delivery, concurrent calls, status reporting, and worker cleanup against FeatBit Cloud.
