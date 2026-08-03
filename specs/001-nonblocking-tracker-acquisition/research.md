# Phase 0 Research: Non-blocking Reply-Tracker Acquisition

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Date**: 2026-08-03

No `NEEDS CLARIFICATION` markers existed in the Technical Context; research resolved the five
design unknowns below. Code references are to the current `main`
(`24306fe`).

## R1. Where acquisition runs relative to the send

**Decision**: Keep the current send → ack → acquire-tracking order; make the acquisition itself
asynchronous.

**Rationale**:
- Measured-latency semantics stay identical: `MessagePublished.sentTimestamp` is recorded at the
  tracking-ready instant, exactly as today (`KafkaRequestReplyAction.scala:56-59`), satisfying
  FR-005 without re-deriving what "sent" means.
- The reply-matching window keeps today's structure (FR-004): the subscription request is issued
  at ack time in both designs, so no new interval appears in which a reply can be lost relative
  to current behavior.
- Send stays on the VU/action path. Gatling actions must not block, and nothing here adds
  blocking to them.

**Alternatives considered**:
- *Acquire before send* (issue's first suggestion): closes a pre-existing latent gap (reply
  produced before the consumer's first assignment can be missed under `auto.offset.reset=latest`)
  but moves `producer.send` into readiness continuations. `KafkaProducer.send` may block up to
  `max.block.ms` (metadata fetch, buffer-full), which would put a potentially-blocking call on
  the setup executor or — worse — the consumer thread. Fixing the latent gap belongs with the
  subscription-lifecycle rework (#165), not here.
- *Blocking acquisition on the VU thread before send*: blocks a shared Gatling event-loop thread
  for up to 60 s — trades one shared-thread stall for another.

## R2. Async primitive and executor model

**Decision**: `java.util.concurrent.CompletableFuture[Void]` for readiness; one pool-owned
single-thread daemon `ScheduledThreadPoolExecutor` (name prefix `gatling-kafka-tracker-setup`)
used for both timeout scheduling and slow-path continuations (`whenCompleteAsync`). Shut down in
the existing `actorSystem.registerOnTermination` block in `KafkaMessageTrackerPool`.

**Rationale**:
- Zero new dependencies (constitution Technology Constraints; Scala stdlib `Future` would drag an
  `ExecutionContext` decision into every signature for no gain — completion sources here are
  Java-thread callbacks, not for-comprehensions).
- Explicit thread ownership: completions may fire from the consumer thread
  (`onPartitionsAssigned`), from `markConsumerFailed`, or from the timeout task; bouncing
  continuations to one named executor makes the threading contract auditable and keeps the
  consumer's poll loop free of tracker bookkeeping.
- A single thread suffices: every continuation is O(µs) (CHM compute, `actorOf`, actor tell, KO
  logging). The waiting itself lives in the future, not in a thread, so there is no head-of-line
  blocking across topics (US1 scenario 1 holds even with many topics preparing concurrently).
- `ScheduledThreadPoolExecutor` gives cancellable timeout tasks: the timeout is cancelled when
  readiness completes first, so no garbage timer fires per successful acquisition.

**Alternatives considered**:
- `CompletableFuture.orTimeout`: uses the JVM-global `CompletableFuture.Delayer` thread —
  functional, but pending timeouts survive pool shutdown and the thread is shared process-wide;
  a pool-owned scheduler keeps lifecycle and naming explicit for thread-dump diagnosis.
- Scala `Promise`/`Future`: needs an `ExecutionContext` at every combinator; interop noise for a
  Java-callback-driven flow.
- Gatling's actor `scheduler` (as used by `KafkaMessageTracker`): tied to actor context, not
  available in the pool without threading it through; and it schedules fixed-rate jobs, while we
  need cancellable one-shots plus an execution queue.
- Per-acquisition threads / cached pool running the *blocking* API: under subscription churn
  (#165 makes every request slow-path) thread count balloons; a bounded pool reintroduces
  cross-topic head-of-line blocking.

## R3. Fate of the blocking internal APIs

**Decision**: Replace, don't accumulate. `KafkaMessageTrackerPool.tracker(...)` →
`acquireTracker(...)(onReady, onFailure)`; `DynamicKafkaConsumer.addTopicForSubscription(topic,
timeout): Boolean` → `requestTopicSubscription(topic): CompletableFuture[Void]`. Test call sites
(4 in `DynamicKafkaConsumerSpec`, `KafkaIntegrationSpec`; 1 in `KafkaMessageTrackerPoolSpec`)
migrate to explicit waits on the future from test threads.

**Rationale**:
- A surviving blocking acquisition method is a loaded footgun — the defect being fixed is someone
  calling it from a callback. One way to acquire = the non-blocking way.
- Neither method is published contract: not in the Scala DSL, not in `javaapi`, not in README or
  examples (usage scan: `KafkaRequestReplyAction`, `KafkaComponents`/`KafkaProtocol` wiring, and
  this repo's tests only). Constitution Principle I therefore permits the internal replacement;
  `ExampleSmokeValidation` remains the witness.

**Alternatives considered**: deprecate-and-keep blocking wrappers — rejected: dead main-code kept
alive for tests invites the exact misuse being removed, and Principle III forbids dead code.

## R4. Reproducing the stall deterministically against a real broker

**Decision**: New `TrackerAcquisitionIsolationSpec` (Testcontainers) with its own Kafka container
configured `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. A request-reply against a nonexistent reply
topic then subscribes but never receives an assignment, holding readiness open until the
configured timeout — a genuine, broker-real "slow topic". Scenario: short protocol reply timeout
(~3 s); fire the poisoned-topic request first, a healthy-topic request immediately after, through
one shared protocol/producer.

- **Red (pre-fix)**: the healthy request's completion is delayed by ≈ the poisoned topic's full
  timeout (its ack callback queues behind the blocked producer I/O thread) — assertion "healthy
  request completes ≪ timeout" fails.
- **Green (post-fix)**: healthy request completes in normal time while the poisoned request KOs
  at ~timeout with the message naming topic and duration; a follow-up healthy request and a
  retry against the poisoned topic both behave per FR-003.

**Rationale**: constitution Principle II — consumer lifecycle and timeout behavior must be
exercised against a real broker; an unassignable-but-subscribable topic is the one condition that
produces an honest indefinite-readiness state without mocking or clock games.

**Alternatives considered**:
- Latency injection into `DynamicKafkaConsumer` via a test seam — a mock of exactly the behavior
  Principle II says not to mock.
- Pausing the broker container mid-test — flaky timing, and stalls *all* topics including the
  healthy one, invalidating the isolation assertion.
- Asserting on producer-thread identity (that the callback thread never parks) — brittle against
  Kafka client internals; behavioral latency assertions test what the spec actually promises.

## R5. Retry-after-timeout semantics

**Decision**: Preserve current mechanics unchanged: a timed-out acquisition leaves the topic
queued for subscription; a later `acquireTracker` for the same topic issues a fresh readiness
request. If the consumer has meanwhile subscribed the topic, `updateSubscription`'s
already-subscribed branch (`DynamicKafkaConsumer.scala:105-106`) completes the new future on the
next poll cycle (≤ 1 s).

**Rationale**: FR-003 requires that a later request *can attempt* preparation again and that
nothing is permanently poisoned — both hold today at the queue level and keep holding with
futures substituted for latches. The known case where a retry can still time out spuriously
(add+remove coalesced into a no-op `subscribe`, readiness never signalled) is issue #164 —
adjacent by design, not a prerequisite, and this change neither fixes nor worsens it.

**Alternatives considered**: cancelling the queued topic on timeout (rollback) — racy against the
consumer thread applying the very same queue entry, and it would *change* subscription-lifecycle
behavior that #164/#165 own; rejected as scope creep.
