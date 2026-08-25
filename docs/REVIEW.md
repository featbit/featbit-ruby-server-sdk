# Ruby Server SDK review

## Scope

The review covers API safety, FeatBit wire compatibility, evaluation correctness, concurrency, shutdown, memory retention, redundant work, documentation, examples, and packaging. The .NET Server SDK is the documentation and lifecycle reference; the Python Server SDK and FeatBit evaluation-server models are protocol references.

## Findings and fixes

| Priority | Area | Finding | Resolution |
| --- | --- | --- | --- |
| Critical | WebSocket | Connection-token encoding called a method that Ruby strings do not implement. A broad rescue returned the raw environment secret, so local mocks passed while FeatBit Cloud rejected the connection. | Corrected number slicing and added a test that decodes the token, reconstructs the secret, and validates its timestamp. |
| Critical | WebSocket | `websocket-client-simple` executes callbacks with the socket as `self`; callbacks that captured synchronizer instance variables failed only with the real transport. | Bound synchronizer methods before registering callbacks and tested the gem's callback execution semantics. |
| High | Events | Typed boolean/number/JSON evaluation values were sent directly, while FeatBit's event model requires `variation.value` to be a string. | Kept typed public results and added wire-only serialization for event values. |
| High | Events | Explicit flush could drain more than the configured batch size into one HTTP request. | Partitioned every delivery by `events_batch_size`, including explicit flush and close. |
| High | Shutdown | Flush/stop commands shared the bounded event queue and could block forever when it was full. | Added a separate unbounded control queue, acknowledgements, thread-safe idempotent close, and queue-full shutdown tests. |
| High | Data store | A single global version rejected valid updates to different entities that shared a timestamp. | Track versions per flag and segment while retaining the global snapshot version. |
| High | Evaluator | Mutually recursive segments could overflow the stack. | Added per-evaluation cycle detection and a cyclic-segment regression test. |
| Medium | Immutability | Nested custom user attributes remained mutable after client construction. | Deep-copied and recursively froze nested hashes, arrays, and strings. |
| Medium | Reconnect | A failed socket was not always closed before reconnect. | Close failed transports and suppress expected close-time errors. |

## Concurrency and resource review

- Data-store snapshots and version maps are mutex protected and defensively copied.
- Evaluation remains local and performs no network I/O.
- Event producers never wait for queue capacity; overflow is counted in `dropped_events`.
- Listener collections are copied under lock and invoked outside the lock.
- Client and event-processor shutdown are safe under concurrent repeated calls.
- Rails creates one lazy client per worker process, avoiding a WebSocket opened before a process fork.
- `scripts/resource_audit.rb` exercises 100 event-processor lifecycles and 80,000 evaluations across 16 threads. It fails on named worker-thread leaks, concurrent errors, or material post-GC heap retention.

## Redundancy review

No duplicate transport or evaluator implementation remains. Small normalization helpers stay component-local where merging them would couple wire parsing, storage, and evaluation behavior. The Console and Rails examples share the public SDK API; Rails-specific singleton and process-lifecycle code remains isolated in `FeatBitClientRegistry`.

## Verification matrix

| Check | Coverage |
| --- | --- |
| Unit/integration tests | Options, data store, evaluator, WebSocket messages, events, status, public exception boundary, examples |
| Static analysis | Entire gem, specs, scripts, Console and Rails example Ruby files |
| Resource audit | 80,000 concurrent evaluations, heap-retention threshold, 100 start/stop cycles, worker-thread cleanup |
| FeatBit Cloud | Server-key authentication, full WebSocket sync, remote flag evaluation, event track/flush, concurrent access, ready status, clean close |
| Packaging | Gem build and metadata validation |

The live check reads credentials only from environment variables. Secrets are not printed, persisted, or committed.
