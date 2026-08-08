# Implementation Plan: Reply Correlation Correctness

**Branch**: `004-reply-correlation-correctness` | **Date**: 2026-08-07 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-reply-correlation-correctness/spec.md`

## Summary

Three defects make a request-reply run report numbers that are wrong rather than missing. All three
are small, local changes with disproportionate blast radius, and the bulk of the work is proving them.

1. **Keyless correlation** (#167). `KafkaAction` substitutes `Array.emptyByteArray` for an absent key,
   and `KafkaMessageTracker.matchKeyFor` folds `null` into that same empty array. Every keyless
   request-reply request therefore registers under one `MatchKey`. Fix: carry the key as `null` to the
   `ProducerRecord`, stop folding `null` into empty in the tracker, and fail a request whose matcher
   yields no identity *before* it is acquired or sent.
2. **Absent payloads** (#168). `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer` call
   `msg.value.length` unguarded; `completeMatched` wraps check execution in `try`/`finally` with no
   `catch`. A tombstone therefore throws out of `Check.check` and the virtual user is never continued.
   Fix: null-guard the three preparers behind `safely(...)` like their siblings, and add a terminal
   catch so no check can strand a user.
3. **Channel readiness** (#193). `completeAssignedReadiness` completes readiness from
   `consumer.assignment()`, which precedes fetch-position resolution, while the plugin defaults
   `auto.offset.reset` to `latest`. Fix: resolve `consumer.position(tp)` on the poll thread for each
   assigned partition of the awaited topic before completing its readiness futures.

The partition-distribution half of #167 falls out of change 1 for free: a `null` key restores Kafka's
round-robin partitioner, which `murmur2` of an empty array had been defeating.

**No published signature changes.** `KafkaProtocolMessage.key` is already `Array[Byte]` and already
nullable on the consume side (`KafkaProtocolMessage.from` copies `consumerRecord.key()` verbatim); the
produce side simply was not using that. Observable behaviour does change, in three ways, and that is
governed by Principle I — see [Constitution Check](#constitution-check).

## Technical Context

**Language/Version**: Scala 2.13 on sbt; Java 17+ (Temurin in CI)

**Primary Dependencies**: Gatling 3.13.5 (`provided`), Kafka clients 7.9.5-ce, kafka-streams-scala.
Avro4s 4.1.2 and Confluent serdes 7.9.8 stay `provided`/optional. No new dependency.

**Storage**: N/A — correlation state is in-memory, actor-private (`mutable.HashMap[MatchKey, MessagePublished]`)

**Testing**: munit + Testcontainers (`ConfluentKafkaContainer`) for integration; Gatling simulations
against the `docker-compose.kafka.yml` stack for load-level proof; `ExampleSmokeValidation` for API
construction

**Target Platform**: JVM library published to Sonatype, consumed by Gatling simulations

**Project Type**: Library — Gatling protocol plugin (single sbt project, Scala core + Java facade)

**Performance Goals**: No regression in reply throughput. The consume path is single-threaded and
gates every reply, so per-reply work must not grow; the readiness change runs once per topic
assignment, not per record.

**Constraints**: The consumer poll thread is the only thread that may touch the `KafkaConsumer`.
`consumer.position(tp)` may block on a `ListOffsets` round trip, so it needs a bounded timeout and
must not be called from the virtual-user thread. Reply topics are `Expression[String]` and may derive
from session data, so dynamic subscription must keep working.

**Scale/Scope**: 6 production files touched, ~120 lines changed. Verification is the larger half:
2 new integration specs, 2 amended simulations, 1 simulation wired into CI, 2 broker definitions.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility** — **No signature change.** `KafkaProtocolMessage.key` stays
      `Array[Byte]`; no DSL, `javaapi`, or serialized-format signature moves. **Observable behaviour
      does change** in three ways, all deliberate and all approved during specification (spec
      Assumptions): a keyless request-reply request under key matching now KOs at issue time instead
      of matching wrongly; a reply with an absent payload now KOs instead of stalling; keyless
      messages now spread across partitions instead of landing on one. Migration Guide entry in
      `README.md` is required in the same PR (FR-017) and is a task, not a follow-up. Version impact:
      minor (`feat`) with a documented behaviour change, not `!:` — no consumer's code stops
      compiling. `ExampleSmokeValidation` stays green and is part of the gate.
- [x] **II. Real Broker Over Mocks** — Every correlation, readiness and timeout claim is proven
      against a real broker: Testcontainers for the integration specs, the Compose stack for the
      Gatling simulations, with #196's echo responder as the oracle. Mocks are used nowhere in this
      feature. The one unit-level spec added (preparer null-guards) has no Kafka interaction, which is
      exactly the permitted case.
- [x] **III. Layer Separation & Single Wire Contract** — `KafkaSender`, `KafkaMessageTracker` and
      `DynamicKafkaConsumer` keep their responsibilities: the readiness change stays inside the
      consumer, the identity change inside the tracker and the action, the payload change inside the
      preparers. `KafkaProtocolMessage` and `KafkaMatcher` are used as-is — neither is extended, and
      no parallel type is introduced. No new abstraction: every change has exactly one caller.
- [x] **IV. Test-First for Behavior Change** — Each of the four user stories gets a test written to
      fail against pre-change code first (FR-025). One caveat is recorded honestly in
      [research.md](./research.md) §R4: for User Story 3 the red-before gate is the new integration
      spec, **not** removing the broker tuning, because that removal was already verified green.
- [x] **V. One Concern per Change, Always Green** — Spec artifacts (spec/plan/research/data-model/
      contracts/quickstart/tasks) land first as one `docs(speckit):` commit. Then one semantic commit
      per issue: `fix(actions)` for #167, `fix(checks)` for #168, `feat(client)` for #193, each green
      under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`, each carrying `Closes #NNN` and the
      `v1.2.0 Reply correlation correctness` milestone. Docs/Migration Guide ride with the commit
      whose behaviour they describe, since FR-017 makes them part of that change rather than a
      separate concern.
- [x] **Constraints** — No new dependency, no upgrade. Avro/Schema Registry stay `provided`. No
      supported Gatling version changes, so the README compatibility table is untouched.

**Result: PASS.** No violations, so Complexity Tracking stays empty.

### Post-Design Re-Check (after Phase 1)

Re-evaluated against the completed design artifacts. **Still PASS**, with two things the design
surfaced that the pre-design pass could not have known:

- **Principle I held better than expected.** Phase 1 confirmed `KafkaProtocolMessage.key` and `.value`
  are already nullable on the consume side, so the whole feature lands with zero signature movement
  and no deprecation cycle. Recorded in [data-model.md](./data-model.md) §1 and §4.
- **Principle III got easier, not harder.** No new type, no new abstraction, no extension of
  `KafkaProtocolMessage` or `KafkaMatcher`. Every change reinterprets existing values —
  `matchKeyFor` loses a branch rather than gaining one.
- **Principle IV needed a correction, and got one.** The pre-design pass assumed removing the broker
  tuning would provide User Story 3's red-before gate. [research.md](./research.md) §R4 found that
  assumption false against the working tree and moved the gate to `PositionedReadinessSpec`. That spec
  was then redesigned (§R5) from a statistical marker race into a direct measurement of the gap —
  `first-delivered <= produced-at-readiness` — so it is decisive on a single run. A check that can pass
  by luck is not a gate, so it does not get to be one here.
- **Principle II unchanged**: the one new unit spec covers preparers, which have no Kafka interaction
  — the explicitly permitted case. Everything else is Testcontainers or the Compose stack.

## Project Structure

### Documentation (this feature)

```text
specs/004-reply-correlation-correctness/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── behavior-contract.md   # Phase 1 output — observable contract deltas
├── checklists/
│   └── requirements.md  # from /speckit-specify
└── tasks.md             # /speckit-tasks — NOT created here
```

### Source Code (repository root)

```text
src/main/scala/org/galaxio/gatling/kafka/
├── actions/
│   ├── KafkaAction.scala                  # [US1/US4] key.getOrElse(empty) → key.orNull
│   └── KafkaRequestReplyAction.scala      # [US1] fail-fast when the matcher yields no identity
├── checks/
│   └── KafkaMessagePreparer.scala         # [US2] null-guard the three unguarded preparers
├── client/
│   ├── KafkaMessageTracker.scala          # [US1] matchKeyFor: drop null→empty folding
│   │                                      # [US2] completeMatched: terminal catch
│   └── DynamicKafkaConsumer.scala         # [US3] resolve fetch position before readiness
└── request/
    └── KafkaProtocolMessage.scala         # [US1] doc only — key nullability made explicit

src/test/scala/org/galaxio/gatling/kafka/
├── checks/
│   └── KafkaMessagePreparerSpec.scala     # [US2] NEW — unit, no Kafka interaction
├── client/
│   └── KafkaMessageTrackerSpec.scala      # [US1] null ≠ empty MatchKey
├── integration/
│   ├── KeylessCorrelationSpec.scala       # [US1] NEW — Testcontainers
│   └── PositionedReadinessSpec.scala      # [US3] NEW — Testcontainers, the red-before gate
└── examples/
    ├── KafkaGatlingTest.scala             # [US1/US2/US4] new scenarios + pinned assertions
    └── KafkaConcurrencyLoadTest.scala     # [US1] wire into CI (30 concurrent users already)

docker-compose.kafka.yml                   # [US3] drop rebalance tuning; [US4] multi-partition topic
.github/workflows/ci.yml                   # [US3] drop rebalance tuning; [US1] run concurrency test
README.md                                  # [FR-017] Migration Guide entry
```

**Structure Decision**: Existing single-project Scala/sbt layout, unchanged. Each user story maps to
one production concern and one verification concern, in the directories that already own them.

## Implementation Approach

### US1 — Correlation identity (#167, P1)

**Production changes (3 files, ~15 lines):**

1. `KafkaAction.resolveToProtocolMessage` — `key.getOrElse(Array.emptyByteArray)` → `key.orNull`.
   `KafkaProtocolMessage.toProducerRecord` already passes `key` straight through, so a `null` key
   reaches the partitioner and round-robin is restored. This single line delivers US4 as well.
2. `KafkaMessageTracker.matchKeyFor` — drop the `if (m == null) Array.emptyByteArray else m`
   substitution. `java.util.Arrays.hashCode(null) == 0` and `Arrays.hashCode(Array.empty) == 1`, and
   `Arrays.equals(null, Array.empty) == false`, so `null` and empty become distinct `MatchKey`s with
   no extra code. This is what FR-001 requires.
3. `KafkaRequestReplyAction.sendKafkaMessage` — after `val id = matcher.requestMatch(protocolMessage)`,
   if `id == null`, call the existing `reportFailure(...)` with a message naming the missing identity
   and return, **before** `acquireTracker` and before `sender.send`. Nothing is registered and nothing
   is published, so the existing "every exit goes through the tracker" invariant is not weakened —
   this exit happens strictly earlier than registration.

**Deliberately not changed**: an *empty* identity still registers and is still tracked. Two concurrent
requests sharing an empty identity collide, and the existing displacement failure (added for #191)
reports that accurately as a reused match id. That is the FR-001 distinction working as intended: an
absent identity is a configuration error reported immediately; a non-unique identity is a simulation
error reported on collision.

**Consume side needs no change**: `MessageConsumed` already rejects `replyId == null` before matching,
so a keyless reply cannot match the empty bucket once the publish side stops creating one.

### US2 — Absent payloads (#168, P2)

**Production changes (2 files, ~25 lines):**

1. `KafkaMessagePreparer` — wrap `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer` in
   `safely(...)` and guard `msg.value == null`, producing a `Validation` failure naming the absent
   payload. `xmlPreparer` and `avroPreparer` already do this; the three are brought to the same shape
   rather than given a new one. A **present-but-empty** value keeps its current behaviour exactly
   (`""` / `Array.emptyByteArray`, both successes) — FR-010.
2. `KafkaMessageTracker.completeMatched` — add a `catch` to the existing `try`/`finally` that reports a
   KO and continues the virtual user. This is defence in depth: it makes FR-008's "every request
   reaches a terminal outcome" true for *any* throwing check, not only for the three being fixed.

### US3 — Positioned readiness (#193, P3)

**Production change (1 file, ~25 lines):**

`DynamicKafkaConsumer.completeAssignedReadiness` — before completing a topic's readiness futures,
resolve the fetch position for each assigned partition of that topic via `consumer.position(tp, timeout)`.
Runs on the poll thread only, which the method already documents and requires. Called from
`onPartitionsAssigned` (inside `poll()`, where Kafka explicitly permits `position`/`seek`) and from the
tail of `updateSubscription()` (also the poll thread).

A `position()` failure or timeout must fail *that topic's* readiness futures, not latch
`markConsumerFailed` — poisoning the pool over one slow `ListOffsets` is the #143 failure mode in a new
costume. See [research.md](./research.md) §R3.

**Broker definitions**: remove `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` from `docker-compose.kafka.yml`
and `.github/workflows/ci.yml` (FR-022). Read §R4 first — this removal is hygiene, not the gate.

### US4 — Partition distribution (#167 realism, P4)

No production change beyond US1's `key.orNull`. Work is verification only: a multi-partition topic in
`topic-init` (every topic there is currently `--partitions 1`, and `KAFKA_AUTO_CREATE_TOPICS_ENABLE`
would otherwise default new topics to 1 partition too), plus a scenario publishing keyless messages to
it and an assertion that reads back where they landed.

### Verification plan (FR-018 – FR-025)

| Story | Red-before gate | Level | Where |
|---|---|---|---|
| US1 | Concurrent keyless request-reply loses/misattributes replies | Integration (Testcontainers) | `KeylessCorrelationSpec` (new) |
| US1 | 30 concurrent users, zero reply-loss budget | Gatling simulation | `KafkaConcurrencyLoadTest` — **wire into CI** |
| US1 | Keyless request-reply under key matching KOs at issue time | Gatling simulation | `KafkaGatlingTest` (new scenario) |
| US2 | Tombstone reply → clean KO, not a stalled user | Unit + Gatling simulation | `KafkaMessagePreparerSpec` (new), `KafkaGatlingTest` |
| US3 | Reply published immediately after readiness is received | Integration (Testcontainers) | `PositionedReadinessSpec` (new) |
| US4 | Keyless messages reach every partition | Gatling simulation | `KafkaGatlingTest` (new scenario) |

**Assertion convention** (FR-024): pin exact counts with `is(...)`, never `lte(...)`. `KafkaGatlingTest`
already does this — `global.failedRequests.count.is(1)` plus a named
`details("Request Reply Bytes wo").failedRequests.count.is(1)` — with a comment explaining that `lte`
let a by-design failure silently stop failing. New scenarios extend that pattern; the global count
moves as by-design failures are added, and each new one gets its own named `details(...)` assertion.

**Stall detection** (FR-021): a stalled virtual user produces *no* failure, so a failure-count
assertion alone goes green on a hung run. Every US2 assertion must therefore pair the failure count
with evidence that users completed — the count of a request executed *after* the failing one in the
same scenario.

## Complexity Tracking

> No Constitution Check violations. Table intentionally empty.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| `consumer.position()` blocks the poll thread on a slow `ListOffsets` | Reply throughput stalls for every topic on that consumer | Bounded `position(tp, Duration)`; fail only that topic's readiness on timeout; §R3 |
| Removing the rebalance tuning does not turn CI red pre-fix | US3 appears proven when it is not | The gate is `PositionedReadinessSpec`, not the removal; stated in §R4 and in the task list |
| ~~`PositionedReadinessSpec` is statistical~~ | — | **Resolved during design.** Redesigned to measure the gap (`F <= S`) instead of racing into it, so it is decisive on a single run; §R5 |
| `KafkaConcurrencyLoadTest` in CI adds ~2 min and a new flake surface | Slower, noisier CI | It already carries a zero-loss budget and passes locally; run it in the existing Gatling step, not a new job |
| Keyless key-matched scenarios in the wild start failing | Downstream simulations go red on upgrade | Deliberate (spec Assumptions); Migration Guide entry naming the two fixes — set a key, or switch to `matchByValue`/`matchByMessage` |
| A `null` key reaching code that assumed non-null | NPE somewhere unrelated | `describeBytes` already handles null (used on the consume path); audit `key` uses during implementation |
