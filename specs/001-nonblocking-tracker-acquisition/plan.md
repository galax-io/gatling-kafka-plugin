# Implementation Plan: Non-blocking Reply-Tracker Acquisition for Request-Reply Sends

**Branch**: `001-nonblocking-tracker-acquisition` | **Date**: 2026-08-03 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/001-nonblocking-tracker-acquisition/spec.md`

Fixes [#163](https://github.com/galax-io/gatling-kafka-plugin/issues/163).

## Summary

`KafkaRequestReplyAction` currently performs tracker acquisition inside the producer send
callback. On first use of a reply topic, `KafkaMessageTrackerPool.tracker(...)` reaches
`DynamicKafkaConsumer.addTopicForSubscription(...)`, which parks on a `CountDownLatch` for up to
the protocol reply timeout (60 s default) — on the producer's single I/O thread, stalling delivery
callbacks for every in-flight send in the simulation.

The fix keeps the current send-then-track order but makes readiness asynchronous:

1. `DynamicKafkaConsumer` exposes promise-based subscription readiness
   (`requestTopicSubscription(topic): CompletableFuture[Void]`), completed by the existing
   rebalance-listener / already-subscribed / failure paths instead of counting down latches.
2. `KafkaMessageTrackerPool` replaces blocking `tracker(...)` with callback-based
   `acquireTracker(...)(onReady, onFailure)`. Fast path (tracker exists) invokes `onReady` inline
   — pure non-blocking map ops. Slow path attaches a continuation and a scheduled timeout on a
   pool-owned single-thread daemon `ScheduledExecutorService`; the producer callback never waits.
3. `KafkaRequestReplyAction` wires `onReady` (record `sentTimestamp`, register
   `MessagePublished`) and `onFailure` (existing KO reporting), so acquisition timeout fails only
   the affected request while the shared producer keeps flowing.

No public DSL, `javaapi`, default, or wire-format change. No new dependency (JDK
`java.util.concurrent` only).

## Technical Context

**Language/Version**: Scala 2.13.18 (core), Java 17+ (Temurin in CI); Java facade untouched

**Primary Dependencies**: Gatling 3.13.5 (actor system, StatsEngine, Clock), Confluent Kafka
clients 7.9.2-ccs (`KafkaProducer` callback semantics, `KafkaConsumer` rebalance listener); JDK
`CompletableFuture` + `ScheduledExecutorService` (no new library)

**Storage**: N/A

**Testing**: MUnit + ScalaTest, Testcontainers (real broker; constitution forbids mocking Kafka
behavior), Gatling simulations `KafkaGatlingTest` / `KafkaJavaapiMethodsGatlingTest` via
`docker-compose.kafka.yml` in CI

**Target Platform**: JVM library (Gatling plugin), Linux CI / macOS dev

**Project Type**: Single sbt project — published library

**Performance Goals**: Producer callback path O(µs), allocation-light: fast path stays two CHM
reads + one atomic increment + actor tell; slow path adds one future, one scheduled timeout task,
one small closure per first-use-of-topic (not per request)

**Constraints**: Producer I/O thread and consumer poll thread must never block on plugin
bookkeeping; setup-executor tasks must be non-blocking (µs-scale); reported latency semantics
unchanged (sent timestamp recorded at tracking-ready instant, as today); per-request failure
isolation preserved

**Scale/Scope**: 3 main-source files changed (`KafkaRequestReplyAction`, `KafkaMessageTrackerPool`,
`DynamicKafkaConsumer`), 4 test files touched + 1 new integration spec; sibling issues #143, #164,
#165, #166 explicitly out of scope

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility**: PASS — no public Scala DSL or `javaapi` signature,
      default, or serialized format changes. Replaced methods (`KafkaMessageTrackerPool.tracker`,
      `DynamicKafkaConsumer.addTopicForSubscription`) are internal `client` collaborators wired by
      the protocol layer; they appear in no README example, no `javaapi` type, and no DSL surface
      (verified by usage scan — consumers are `KafkaRequestReplyAction` and this repo's tests
      only). `ExampleSmokeValidation` must stay green as the gate's witness.
- [x] **II. Real Broker Over Mocks**: PASS — the regression scenario (stalled callback), timeout
      isolation, and retry-after-timeout are validated against Testcontainers in a new integration
      spec (broker configured with topic auto-creation disabled to make assignment genuinely
      unavailable). Existing queue-semantics unit specs keep their no-broker scope.
- [x] **III. Layer Separation & Single Wire Contract**: PASS — `DynamicKafkaConsumer` still owns
      subscription readiness, `KafkaMessageTrackerPool` still owns acquisition/refcounting,
      `KafkaSender` untouched, the action only wires injected collaborators. Error propagation
      moves from thrown exceptions to an explicit `onFailure` channel (removes
      control-flow-by-exception from the callback path). No new parallel message/matcher types; no
      speculative abstraction — the future-based readiness has a real caller (the pool) and a real
      forcing function (this defect).
- [x] **IV. Test-First for Behavior Change**: PASS — the plan's primary test reproduces the stall
      against pre-fix code (fails red), plus focused red-first tests for timeout KO isolation and
      async readiness completion. See quickstart.md for the red/green run order.
- [x] **V. One Concern per Change, Always Green**: PASS — spec/plan/tasks artifacts land first as
      `docs(speckit): …`; implementation is a single `fix(client): non-blocking tracker
      acquisition (#163)` commit, green under `sbt scalafmtCheckAll scalafmtSbtCheck compile
      test`; PR carries milestone `v1.1.0 Request-reply reliability` and `Closes #163`.
- [x] **Constraints**: PASS — zero new dependencies; Avro/Schema Registry scope untouched; Gatling
      version unchanged (no README compatibility-table update needed).

**Post-design re-check (after Phase 1)**: all six gates still PASS — design artifacts introduce no
public-surface change and no new dependency; Complexity Tracking stays empty.

## Project Structure

### Documentation (this feature)

```text
specs/001-nonblocking-tracker-acquisition/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0: decisions & alternatives
├── data-model.md        # Phase 1: entities, states, transitions
├── quickstart.md        # Phase 1: validation guide (red/green runs)
├── contracts/
│   └── internal-api.md  # Phase 1: internal API + threading contract
├── checklists/
│   └── requirements.md  # Spec quality checklist (done)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/main/scala/org/galaxio/gatling/kafka/
├── actions/
│   └── KafkaRequestReplyAction.scala      # callback → non-blocking; onReady/onFailure wiring
└── client/
    ├── DynamicKafkaConsumer.scala         # promise-based requestTopicSubscription; latch removal
    └── KafkaMessageTrackerPool.scala      # acquireTracker(onReady, onFailure); setup executor

src/test/scala/org/galaxio/gatling/kafka/
├── client/
│   ├── DynamicKafkaConsumerSpec.scala     # queue semantics migrated to future API
│   ├── KafkaMessageTrackerPoolSpec.scala  # tracker() call site → acquireTracker
│   └── TrackerRefCountSpec.scala          # refcount invariants over async acquisition
└── integration/
    ├── KafkaIntegrationSpec.scala         # existing sync-API sites migrated to future API
    └── TrackerAcquisitionIsolationSpec.scala  # NEW: regression + isolation (red-first)
```

**Structure Decision**: Existing single-project sbt layout; no new modules. One new test file
(`TrackerAcquisitionIsolationSpec`) because the regression scenario needs its own broker container
configured with `auto.create.topics.enable=false`, which the shared container in
`KafkaIntegrationSpec` does not use.

## Design

### Flow after the change (request-reply success path)

```text
VU thread          producer I/O thread              setup executor / consumer thread
────────────       ─────────────────────────        ────────────────────────────────
send(msg) ───────▶ ack callback:
                     requestMatch(msg)      (µs)
                     acquireTracker(...):
                       fast path? ──yes──▶ onReady inline:
                                             sentTimestamp = now
                                             tracker ! MessagePublished
                       └─no (first use) ──▶ requestTopicSubscription(topic)   [enqueue only]
                                            schedule timeout(replyTimeout)
                     ◀── callback returns; producer thread free ──

                                            consumer thread: onPartitionsAssigned
                                              → future.complete()             (µs)
                                            setup executor (whenCompleteAsync):
                                              success → create/register actor,
                                                        onReady → MessagePublished
                                              timeout/failure → onFailure → KO,
                                                        VU continues
```

### Key decisions (full rationale in research.md)

1. **Send-first order preserved** — acquisition still starts at the delivery callback, so
   measured-latency semantics and the reply-matching window are byte-for-byte today's (FR-004,
   FR-005). Acquire-before-send was rejected: it moves `producer.send` onto continuation threads
   (risking `max.block.ms` stalls on the consumer thread) for no requirement the spec needs.
2. **JDK `CompletableFuture` + pool-owned single-thread daemon `ScheduledExecutorService`** — no
   new dependency, explicit thread ownership, shut down with the pool via the existing
   `registerOnTermination` block. Timeout = scheduled `completeExceptionally` with the exact
   message text used today (`Timed out waiting for consumer assignment to topic 'T' after t`).
3. **Blocking internal APIs are replaced, not kept alongside** — a blocking `tracker(...)` left in
   place invites exactly the regression this feature removes. The four test call sites of
   `addTopicForSubscription` migrate to `requestTopicSubscription(...).get(timeout)`-style waits
   (blocking is fine on test threads).
4. **FR-006 is satisfied structurally**: acquisition begins only after a successful ack, so no
   code path holds a tracking reservation for a send that then fails. The pool's
   insert-or-increment `compute` and `releaseTracker` refcount logic are unchanged.
5. **Failure semantics preserved**: consumer failure fails pending futures
   (`markConsumerFailed`), close/shutdown drains the queue exceptionally (spec edge case
   "shutdown while preparation in flight"), and a timed-out topic remains eligible for a fresh
   attempt — the already-subscribed fast-ack in `updateSubscription` still completes retries on
   the next poll cycle. The known no-op-subscribe latch defect stays tracked as #164 and is
   neither fixed nor worsened here.

### Discovered during implementation (design deltas from the original outline)

1. **Readiness must not live in the rebalance listener.** `updateSubscription` captured the pending
   latches (now futures) in the `ConsumerRebalanceListener` closure. Kafka keeps only the most
   recent listener, so a second `subscribe()` issued before the first rebalance completed silently
   discarded the first listener *and the readiness it held* — the caller then waited out its full
   timeout for a topic that was subscribed and healthy. Blocking acquisition hid this by
   serialising subscriptions; making acquisition concurrent exposed it immediately (traced on the
   CI simulation: `Timed out waiting for consumer assignment to topic 'test.t1' after 60 seconds`).
   Readiness therefore lives on the consumer in `awaitingAssignment: topic -> pending futures`, and
   is resolved from `consumer.assignment()` by `completeAssignedReadiness()`, called from the
   listener and after every poll. Side effects: readiness now completes only when the topic is
   genuinely assigned (previously *any* assignment completed *all* pending latches), and
   `subscribe()` is skipped when the topic set is unchanged, since readiness no longer depends on a
   rebalance being triggered.

2. **`docker-compose.kafka.yml` needed `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`.** The test
   broker was inheriting Kafka's 3 s default, so the first assignment of every new consumer group
   took ~3 s. `KafkaGatlingTest` has no echo service — its "replies" are published by sibling
   scenarios at +1 s and +2 s — so with `auto.offset.reset` at its `latest` default those replies
   landed before the reply consumer was assigned and were skipped. The simulation passed only
   because the blocking acquisition froze the shared producer I/O thread and thereby also delayed
   the sibling scenario's publish until after the assignment: it was green *because of* the defect
   under fix. Zeroing the delay on the test broker (what Testcontainers' own Kafka module does)
   makes the simulation deterministic instead of dependent on that stall. Verified: KO rate is back
   to the baseline 1-of-9 (11.11%), the one KO being `scnRRwo`, which is failing by design.

3. **Making acquisition non-blocking moved failure handling, and the first cut lost some of it.** A
   review pass over this branch found several guarantees that the pre-fix code held only because the
   producer's I/O thread serialised everything, and that the asynchronous version had to re-establish
   explicitly:
   - The action's `try`/`catch` around acquisition was deleted, but `acquireTracker` can still throw
     synchronously (`RejectedExecutionException` once the setup executor is shut down). The slow path
     is now wrapped and reports through `onFailure`; both paths hand the tracker over via
     `deliverReady`, which keeps a throwing `onReady` from escaping into the delivery callback and
     from being reported as a second, contradictory outcome for a request already in the mailbox.
   - `registerTracker` created the tracker actor *before* the deduplicating `compute`. With callbacks
     no longer serialised, N concurrent first uses of a topic all reach it and N-1 actors were started
     and orphaned, each still running its own periodic timeout scan. The actor is now created inside
     the insert branch.
   - `close()` drains `topicsQueue` from its own thread, so `updateSubscription`'s
     `val (topic, readiness) = topicsQueue.poll()` could destructure `null` and take the whole pool
     down through `markConsumerFailed`. Null-guarded.
   - The readiness continuation did not re-check `consumerFailure`, dropping a guard the pre-fix code
     had on both sides of the blocking wait; and `setupExecutor.shutdownNow()` discarded exactly the
     continuations the shutdown ordering exists to preserve (a `ScheduledThreadPoolExecutor` cancels
     even zero-delay queued tasks on shutdown unless told otherwise). Both restored.
   - Readiness parked for a topic that is then unsubscribed is now failed explicitly instead of
     waiting out the caller's full timeout on a topic nobody listens to.

   Two review findings are deliberately left open: readiness now requires the topic in this member's
   own `assignment()`, which changes behaviour for a user-supplied shared `group.id` (a decision, not
   a repair), and `awaitingAssignment` still retains entries for readiness resolved purely by the
   pool's timeout.

### Explicitly out of scope (sibling milestone issues)

- #143 — poll after full unsubscribe crashes the consumer and poisons the pool
- #164 — coalesced add+remove produces a no-op `subscribe`, readiness never signals
- #165 — per-request subscription churn / rebalance
- #166 — tracker timer + actor leak on release

This feature removes the *amplification* of slow readiness across the shared producer; the
siblings remove the *causes* of slow readiness.

## Complexity Tracking

No constitution violations — table intentionally empty.
