# Implementation Plan: Idle-Released Reply Channels for Request-Reply

**Branch**: `002-hold-reply-subscriptions` | **Date**: 2026-08-04 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/002-hold-reply-subscriptions/spec.md`

Fixes [#165](https://github.com/galax-io/gatling-kafka-plugin/issues/165).

## Summary

Request-reply releases a reply channel the moment its last in-flight request completes:
`KafkaRequestReplyAction` wires `MessagePublished.onComplete` to `KafkaMessageTrackerPool.releaseTracker`,
which decrements a refcount, removes the `(topic, matcher)` entry at zero and unsubscribes the topic.
In a sequential scenario the refcount is always 1, so **every** reply tears the channel down and the
next request rebuilds it — one consumer-group rebalance per request.

The refcount is not the bug. It accurately measures requests in flight; it is a poor predictor of
future use, and the code reads "nothing in flight" as "never needed again". This change keeps the
refcount and the release machinery and moves the **trigger**:

1. `KafkaMessageTrackerPool` — reaching zero records `idleSince` instead of removing the entry. A
   pool-owned sweep on the setup executor releases channels idle beyond `idleGraceMillis`
   (default 30 s) and unsubscribes topics left with no channels. The fast path also re-reads
   `consumerFailure` before handing a tracker over.
2. `DynamicKafkaConsumer` — never unsubscribes down to an empty set; a consumer with no subscription
   and no assignment fails on its next poll and takes the pool with it (#143).

Rejected: deleting the release outright. It fixes #165 and reverts #78 — see research.md R1/R2.

No public DSL, `javaapi`, default, wire-format or protocol-option change; no constructor signature
change; no new dependency.

## Technical Context

**Language/Version**: Scala 2.13 (core), Java 17+ (Temurin in CI); Java facade untouched

**Primary Dependencies**: Gatling 3.13.5 (actor system, StatsEngine, Clock), Kafka clients
7.9.5-ce (`KafkaConsumer` subscribe/rebalance semantics, group.initial.rebalance behavior); no
new library

**Storage**: N/A

**Testing**: MUnit 1.3.4 + ScalaTest, testcontainers-scala 0.44.1 (real broker; constitution
forbids mocking Kafka behavior), Gatling simulations `KafkaGatlingTest` /
`KafkaJavaapiMethodsGatlingTest` via `docker-compose.kafka.yml` in CI

**Target Platform**: JVM library (Gatling plugin), Linux CI / macOS dev

**Project Type**: Single sbt project — published library

**Performance Goals**: Steady-state request-reply pays zero establishment cost after first use of
a topic (fast path: two CHM reads + actor tell); establishment happens at most once per
`(topic, matcher)` per run; reply-detection latency for other topics is disturbed only by each
topic's first use, never by request completion

**Constraints**: Producer I/O and consumer poll threads keep 001's never-block rules; reported
response-time semantics unchanged (FR-010); held state per `(topic, matcher)` is one subscription
entry + one idle actor — linear in distinct reply topics, accepted and documented (spec
Assumptions); consumer-failure and shutdown semantics unchanged

**Scale/Scope**: 4 main-source files changed (`KafkaMessageTrackerPool`, `DynamicKafkaConsumer`,
`KafkaMessageTracker`, `KafkaRequestReplyAction`); 1 new integration spec, 1 test file deleted,
4 test files edited; sibling issues #143, #164, #166, #191 and the #193 redesign explicitly out
of scope

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility**: PASS — no public Scala DSL, `javaapi`, default, or
      serialized-format change, and no constructor signature change. `releaseTracker`,
      `removeTopicSubscription` and `MessagePublished.onComplete` all survive with their existing
      signatures; only *when* release fires changes. The idle grace is `private[kafka]`, not a
      protocol option and not a constructor parameter — adding one would be a binary break for what
      is a bug fix. `ExampleSmokeValidation` stays the gate's witness.
- [x] **II. Real Broker Over Mocks**: PASS — churn, idle-gap, idle-release, cross-topic and teardown
      scenarios all run against Testcontainers in `TrackerLifetimeSpec`. It was the real broker that
      surfaced #143 on the idle-release path, which no unit test would have shown. Unit-level
      pin-downs (unmatched-discard) stay in the no-Kafka actor spec, their permitted scope.
- [x] **III. Layer Separation & Single Wire Contract**: PASS — sender/tracker/consumer
      responsibilities unchanged; `KafkaProtocolMessage` and `KafkaMatcher` untouched; no new
      parallel types. The pool gains one private sweep; no new abstraction without a caller.
- [~] **IV. Test-First for Behavior Change**: PASS WITH A RECORDED DEVIATION — three structural
      witnesses were observed red before the fix (registration identity across completion, across an
      idle gap, across 50 sequential requests), and the idle-release test was written before its
      implementation. The SC-003 cross-topic test is a forward guard that does not go red pre-fix.
      See Complexity Tracking.
- [~] **V. One Concern per Change, Always Green**: PASS WITH A RECORDED DEVIATION — spec artifacts
      land as their own `docs(speckit): …` commit before the `fix(client):` commit, each green under
      `sbt scalafmtCheckAll scalafmtSbtCheck compile test`; PR carries milestone
      `v1.1.0 Request-reply reliability` and `Closes #165`. #143's trigger is narrowed inside this
      PR — see Complexity Tracking.
- [x] **Constraints**: PASS — zero new dependencies; Avro/Schema Registry scope untouched; Gatling
      version unchanged; no new protocol option (FR-011).

**Post-design re-check**: gates I, II, III and Constraints pass unchanged. Gates IV and V each carry
one deviation, both recorded in Complexity Tracking with the rejected simpler alternative named.

## Project Structure

### Documentation (this feature)

```text
specs/002-hold-reply-subscriptions/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0: decisions & alternatives (R1–R6)
├── data-model.md        # Phase 1: entities, lifetimes, transitions
├── quickstart.md        # Phase 1: validation guide (red/green runs)
├── contracts/
│   └── internal-api.md  # Phase 1: internal API + lifetime contract (H1–H14)
├── checklists/
│   └── requirements.md  # Spec quality checklist (done)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/main/scala/org/galaxio/gatling/kafka/
├── actions/
│   └── KafkaRequestReplyAction.scala      # unchanged from main (onComplete wiring stays)
└── client/
    ├── DynamicKafkaConsumer.scala         # never unsubscribe to an empty set (#143)
    ├── KafkaMessageTracker.scala          # unchanged from main
    └── KafkaMessageTrackerPool.scala      # idleSince on TrackerEntry, idle sweep,
                                           # release no longer removes, fast-path failure re-check

src/test/scala/org/galaxio/gatling/kafka/
├── client/
│   ├── DynamicKafkaConsumerSpec.scala     # removal-queue test deleted; rest unchanged
│   ├── KafkaMessageTrackerSpec.scala      # onComplete probe → next/stats; + FR-008 pin-down
│   ├── KafkaMessageTrackerPoolSpec.scala  # unchanged (fail-fast path)
│   └── TrackerRefCountSpec.scala          # DELETED (mirrors an algorithm that no longer exists)
└── integration/
    ├── KafkaIntegrationSpec.scala         # unsubscribe-capability test deleted; rest unchanged
    ├── TrackerAcquisitionIsolationSpec.scala  # onComplete probe → next-action latch
    └── TrackerLifetimeSpec.scala          # NEW: red-first churn regression + hold semantics
```

**Structure Decision**: Existing single-project sbt layout; no new modules. One new test file
(`TrackerLifetimeSpec`) because the lifetime scenarios drive the real `KafkaRequestReplyAction`
end-to-end and need their own broker tuning (`group.initial.rebalance.delay.ms` raised to make
every re-establishment measurably expensive), distinct from `TrackerAcquisitionIsolationSpec`'s
acquisition-isolation concern.

## Design

### Lifecycle before → after

```text
BEFORE (release on completion)          AFTER (release on idleness)
──────────────────────────────          ───────────────────────────
reply matched → refCount 0              reply matched → refCount 0
  → remove entry, unsubscribe             → idleSince = now
next request → rebuild (rebalance)      next request within grace → reuse, no setup
  ⋮  one rebalance per request          idle past grace → sweep releases + unsubscribes
                                        run end → registerOnTermination tears the pool down
```

### Key decisions (full rationale in research.md)

1. **Idleness, not completion** (R1) — the two statements the refcount conflates are separated by a
   clock. Sequential scenarios re-acquire in milliseconds and keep their channel; per-user reply
   topics go idle and are reclaimed.
2. **Grace is a think-time scale, not a request-latency scale** (R1) — deliberately not derived from
   the reply timeout, which would make the grace shorter than a single establishment whenever the
   timeout is short. Internal to the plugin, not a protocol option, and not a constructor parameter:
   adding one would be a binary break for a bug fix.
3. **The release is off the request path** — it runs on the pool's setup executor, not in the
   producer callback `onComplete` runs on.
4. **Never unsubscribe to empty** (R4) — found by the new test, not predicted. Narrows #143.

### Discovered during implementation

1. **The first version of this feature deleted the release entirely and reverted #78.** Recorded in
   research.md R2, including how the research missed it.
2. **Idle release immediately hit #143**, on the second request of the new test. Handled here because
   this change makes that path routine.
3. **The cost model in the original plan was wrong** — re-establishment costs ~0.6 s, not a full
   `group.initial.rebalance.delay.ms`; only the initial group join pays that. Timing-based assertions
   were rebuilt as structural ones before the fix landed.

### Explicitly out of scope (sibling milestone issues)

- #164 — no-op re-subscription readiness latch (window closed by add-only subscription; issue
  stays open for its own resolution)
- #143 — poll crash after full unsubscribe (trigger removed from production paths; issue stays
  open — the defensive decision belongs to it)
- #166 — per-tracker periodic timer accumulation within a run (held actors make the timer count
  *bounded by distinct topics* instead of unbounded re-creation, but the leak fix is #166's)
- #191 — reply arriving before its request finishes registering (register-before-send is #193's
  step 2)
- #193 — measurement-semantics decision, positioned readiness, `.replyTopics(...)` warm-up

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| **Principle IV** — the SC-003 cross-topic test is a forward guard, not a red-first test | It does not go red against the pre-fix code: re-establishment after the initial group join costs ~0.6 s, so scenario B's churn does not move scenario A's median outside the 1.5× bound in this environment | A genuinely red-first cross-topic test would need an environment where establishment is far more expensive than a round trip. The three structural witnesses (registration identity across completion, across an idle gap, across 50 sequential requests) *are* red-first and cover the same requirement's mechanism; adding a synthetic environment to redden the measurement adds no coverage |
| **Principle V** — #143's trigger is narrowed inside a PR scoped to #165 | Idle release makes "unsubscribe the last topic" a routine event rather than a rare one, and the crash it causes is terminal for the run. Leaving it would ship a fix whose own happy path poisons the pool | Deferring to #143 was rejected: the new idle-release test fails on its second request without this. Only the trigger is narrowed — the defensive behaviour #143 also covers is untouched and the issue stays open |
