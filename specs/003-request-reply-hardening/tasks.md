---

description: "Task list for Request-Reply Reliability Hardening"
---

# Tasks: Request-Reply Reliability Hardening

**Input**: Design documents from `/specs/003-request-reply-hardening/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [contracts/internal-api.md](contracts/internal-api.md)

**Tests**: Per Constitution Principle IV, test tasks are MANDATORY here — all four issues are bug
fixes, so each ships a test that reproduces the defect against pre-change code. Per Principle II,
every Kafka behaviour claim is asserted against Testcontainers or the `docker-compose.kafka.yml`
stack, never a mock.

**Organization**: Tasks are grouped by user story. Story labels map to [spec.md](spec.md):

| Story | Priority | Issue | Phase |
|---|---|---|---|
| **US1** | P1 | #191 — a reported timeout always means the SUT did not answer | Phase 6 |
| **US2** | P1 | #143 — request-reply works no matter how far into the run it starts | Phase 3 |
| **US3** | P2 | #166 — a long run holds only the channels it is still using | Phase 5 |
| **US4** | P3 | #196 — the CI simulation proves a real round trip | Phase 4 |

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- Exact file paths are included in every description

## Path Conventions

Single-module Scala/sbt project (`gatling-kafka-plugin`):

- **Plugin sources**: `src/main/scala/org/galaxio/gatling/kafka/{protocol,actions,client,checks,request}/`
- **Tests**: `src/test/scala/org/galaxio/gatling/kafka/{client,integration,examples}/`
- **Broker definitions**: `docker-compose.kafka.yml`, `.github/workflows/ci.yml`

## ⚠️ Phase order is NOT priority order — this is deliberate

Phases run in the plan's implementation sequence — **US2 → US4 → US3 → US1** — not in P1→P3 order.
Issue #193's own sequencing note asks for #196 (US4) before #191 (US1) so the hardest fix has a
deterministic CI oracle rather than a coincidence to verify against. #143 (US2) leads because it is
fully self-contained and removes a terminal, run-ending failure.

The four stories are genuinely independent: **any one of them can be implemented and shipped alone.**
US1 remains the highest-value single slice (see Implementation Strategy).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish a known-green starting point. Nothing is created — this is an established repo.

- [X] T001 Enable the shared git hooks by running `bash scripts/install-hooks.sh` (once per clone; see `.githooks/pre-commit`)
- [X] T002 Bring up the broker stack with `docker compose -f docker-compose.kafka.yml up -d` and confirm topics `myTopic1, test.t1, myTopic2, test.t2, myTopic3, test.t3, test.t` exist
- [X] T003 Confirm the baseline is green: `sbt scalafmtCheckAll scalafmtSbtCheck compile test`, so every later red is attributable to the change under test

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Anchor the measured baselines that US1 and US3 prove movement against.

**There are no blocking code prerequisites.** The four fixes touch disjoint concerns and none needs
scaffolding from another. This phase exists only to pin the numbers.

- [X] T004 Reproduce the documented reply-loss baseline with one run of `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaConcurrencyLoadTest.scala` (`sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"`) and record the KO count; its `KnownReplyLossBudget` comment documents 0–2 KO of ~6,760, and T043 tightens it to 0

**Checkpoint**: Baseline pinned — all four stories may start, in any order or in parallel.

---

## Phase 3: User Story 2 - Request-reply works no matter how far into the run it starts (Priority: P1) — issue #143

**Goal**: A simulation whose first request-reply request happens minutes into the run is served
normally. The shared reply-receiving machinery never fails because nothing needed it yet.

**Independent Test**: Build a pool with a shortened initialization wait, request no reply topic, let
the wait expire, then request one — it must be served, with no consumer failure logged at any point.

### Tests for User Story 2 (MANDATORY — Principle IV) ⚠️

> Write these first and confirm they FAIL against pre-change code.

- [X] T005 [P] [US2] Add a test to `src/test/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumerSpec.scala` asserting a consumer whose initialization wait expires with no topic requested neither throws nor invokes `onFailure` (contract C1, C3; red: `IllegalStateException: Consumer is not subscribed to any topics or assigned any partitions`)
- [X] T006 [P] [US2] Create `src/test/scala/org/galaxio/gatling/kafka/integration/ConsumerStartupSpec.scala` — Testcontainers spec that builds a `KafkaMessageTrackerPool` with a short initialization wait, requests no topic, lets the wait expire, then acquires a tracker for a real topic and drives one request-reply through (contracts C2, P1, P2; SC-003)
- [X] T007 [P] [US2] Add a test to `src/test/scala/org/galaxio/gatling/kafka/integration/ConsumerStartupSpec.scala` asserting a pool built but never used logs no consumer failure through to shutdown (contract P2; SC-004)

### Implementation for User Story 2

- [X] T008 [P] [US2] Add an overloaded `apply` taking `initializationTimeout: FiniteDuration` to the `DynamicKafkaConsumer` companion in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala`, keeping the existing `apply` delegating with the 90 s default (research R7 — additive, no signature break)
- [X] T009 [US2] Add a secondary constructor taking `initializationTimeout` to `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala`, passing it to the consumer; the primary constructor stays byte-identical (research R7)
- [X] T010 [US2] In `DynamicKafkaConsumer.run()` (`src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala`), capture the boolean `initLatch.await(...)` returns and log at debug that the wait expired with no reply topic requested (contract C4)
- [X] T011 [US2] Guard the poll in `run()` in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala`: skip `consumer.poll(...)` whenever `consumer.subscription()` and `consumer.assignment()` are both empty, sleeping the poll interval instead, and still call `updateSubscription()` on that turn so a later topic is picked up (contracts C1, C2 — research R1)
- [X] T012 [US2] Confirm the 002-era "never unsubscribe down to nothing" guard at `DynamicKafkaConsumer.scala:176-181` is untouched and `src/test/scala/org/galaxio/gatling/kafka/integration/KafkaIntegrationSpec.scala`'s unsubscribe test passes unchanged (contract C5)
- [X] T013 [US2] Run `sbt scalafmtAll scalafmtSbt` then `sbt scalafmtCheckAll scalafmtSbtCheck compile test`; commit as `fix(client): do not poll a consumer with nothing to receive on (#143)` with the `v1.1.0 Request-reply reliability` milestone and `Closes #143`

**Checkpoint**: US2 complete and independently verifiable.

---

## Phase 4: User Story 4 - The CI simulation proves a real round trip (Priority: P3) — issue #196

**Goal**: Every request-reply scenario in the CI gate is answered by a responder that received its
request, and the one scenario designed to time out does so on a topic nobody serves.

**Independent Test**: Delete every produce-only scenario from `setUp` and run the simulation — all
request-reply scenarios must still pass (SC-007).

### Tests for User Story 4 (the simulation *is* the test) ⚠️

- [X] T014 [P] [US4] Add `myTopic4` to the topic-creation command in `docker-compose.kafka.yml` (contract G6)
- [X] T015 [P] [US4] Add `myTopic4` to `KAFKA_CREATE_TOPICS` in `.github/workflows/ci.yml`, leaving the pre-existing `test.t` divergence between the two lists alone — that is issue #192's subject (contract G6)

### Implementation for User Story 4

- [X] T016 [US4] Add an echo responder to `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` in `KafkaConcurrencyLoadTest`'s shape — a `KafkaSender` driven from a `DynamicKafkaConsumer`, started in `before` behind a readiness probe, closed in `after` and on a `before` failure — consuming `myTopic1` and `myTopic2` and replying on `test.t1` and `test.t2` respectively (contract G1; data-model §4)
- [X] T017 [US4] Make the echo preserve key and value byte-for-byte in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`, so `scnRR`'s `jsonPath("$.m").is("dkf")` and `scnRR2`'s `matchByValue` + `bodyBytes.is("tstBytes")` all pass unchanged (contract G2)
- [X] T018 [US4] Attach a response-timestamp header to the reply in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` using `KafkaProtocolMessage`'s existing `headers: Option[Headers]` field and a `RecordHeaders` — never the key or value (contract G3)
- [X] T019 [US4] Point `scnRRwo`'s `requestTopic` at `myTopic4` in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`, leaving its reply topic `test.t2`, so the responder never answers it (contract G4)
- [X] T020 [US4] Replace the assertion in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` with `global.failedRequests.count.is(1)` plus `details("Request Reply Bytes wo").failedRequests.count.is(1)`, and update the explanatory comment (contract G5; SC-008)
- [X] T021 [US4] Log responder send failures loudly in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` — a silently-stopped responder is indistinguishable from the plugin losing replies (contract R4)
- [X] T022 [US4] Verify SC-007 by hand: comment out `scn`, `scn2`, `scnAvro4s` and `scnwokey` from `setUp` in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`, run the simulation, confirm the request-reply scenarios still pass, then restore `setUp` — do not commit the variant
- [X] T023 [US4] Answer SC-009 by observation: run `KafkaGatlingTest` with and without `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` in `docker-compose.kafka.yml`, and record the outcome in the PR description either way (research R6 expects it to still be required, because #193 point 5 is unfixed — but the issue asks for a measurement, not a prediction)
- [X] T024 [US4] Run `sbt scalafmtAll scalafmtSbt`, the default gate, and `sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"`; commit as `test(examples): answer request-reply with a real echo responder (#196)` with the milestone and `Closes #196`

**Checkpoint**: US4 complete. The CI gate now has a real oracle for US1.

---

## Phase 5: User Story 3 - A long run holds only the channels it is still using (Priority: P2) — issue #166

**Goal**: Releasing a reply channel releases everything scoped to it — the periodic timeout scan
stops and the tracker is no longer retained.

**Independent Test**: Create and retire at least 20 reply channels in sequence; once all are idle past
their grace, live scan tasks must equal the channels currently held (zero), not the 20 created.

### Tests for User Story 3 (MANDATORY — Principle IV) ⚠️

- [X] T025 [P] [US3] Add tests to `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala`: `Stop` cancels the periodic scan and the actor drops later messages (contract T7), and `Stop` is safe on a tracker that never armed a scan because every request had `replyTimeout <= 0` (contract T8)
- [X] T026 [P] [US3] Add a test to `src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala` asserting that after a channel is released as idle, no further `TimeoutScan` activity occurs for that topic and the entry is gone (contracts P3, E4; red: the scan keeps firing once per second for the rest of the run)
- [X] T027 [P] [US3] Add a test to `src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala` driving at least 20 sequential reply topics through acquire → release → idle grace, asserting live scan tasks track channels currently held rather than channels ever created (contract P5; SC-005, SC-006)

### Implementation for User Story 3

- [X] T028 [US3] Add `case object Stop` to the sealed `TrackerMessage` trait in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` (additive — the trait is sealed, so nothing external extends or exhaustively matches it)
- [X] T029 [US3] Retain the `Cancellable` returned by `scheduler.scheduleAtFixedRate` in `triggerPeriodicTimeoutScan` in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` instead of discarding it, and have the `Stop` handler cancel it and return `die` (contracts T7, T8 — research R2: Gatling's `ActorSystem` has no `stop`, so a message is the only way)
- [X] T030 [US3] Send `Stop` from `sweepIdleTrackers` in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala` after the entry is removed and outside the `computeIfPresent` lambda, following the precedent the existing `doUnsubscribe` handling already sets (contracts P3, P4, E1)
- [X] T031 [US3] Send `Stop` to every entry the consumer-failure broadcast clears in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala`, so timers do not outlive a pool that can no longer be used (contracts P3, E3)
- [X] T032 [US3] Send `Stop` to any entry still held at pool shutdown in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala`'s `registerOnTermination` block (contracts P3, E3)
- [X] T033 [US3] Run `sbt scalafmtAll scalafmtSbt` then the default gate plus `TrackerLifetimeSpec`; commit as `fix(client): stop the tracker and its timeout scan on release (#166)` with the milestone and `Closes #166`

**Checkpoint**: US3 complete. The tracker lifecycle is correct before US1 builds on it.

---

## Phase 6: User Story 1 - A reported timeout always means the SUT did not answer (Priority: P1) 🎯 — issue #191

**Goal**: Every reply the plugin receives is matched to its request. A reported reply timeout means
the system under test did not answer — never that the tool dropped an answer it had.

**Independent Test**: Sustained request-reply load against an echo responder answering far faster than
the reply timeout — zero requests reported as reply timeouts.

- [X] T034 [US1] **BLOCKING** — obtain maintainer approval for the behaviour change recorded in the Complexity Tracking table of `specs/003-request-reply-hardening/plan.md`: after this change a failed tracker acquisition reports the same KO **without publishing the request**. Principle I requires this be proposed and approved, not arrive as a side effect

### Tests for User Story 1 (MANDATORY — Principle IV) ⚠️

- [X] T035 [P] [US1] Add a test to `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala` sending `MessagePublished` → `MessageConsumed` → `MessageAcked` in that order, asserting the reply is held and then completed, and that the logged response starts at the **ack** timestamp (contracts T1, T2; red: `sentMessages.remove` returns `None` and the reply is discarded)
- [X] T036 [P] [US1] Add a test to `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala` asserting `TimeoutScan` measures from registration, so a record whose `MessageAcked` never arrives still times out at `replyTimeout` (contract T3)
- [X] T037 [P] [US1] Add a test to `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala` asserting `SendFailed` removes the record, logs a KO, and invokes `onComplete` exactly once so the channel's in-flight count returns to balance (contracts T4, T5)
- [X] T038 [P] [US1] Create `src/test/scala/org/galaxio/gatling/kafka/integration/ReplyRegistrationRaceSpec.scala` — Testcontainers spec with an in-process echo responder answering far faster than the reply timeout, driving the real `KafkaRequestReplyAction` over enough requests to hit the window repeatedly, asserting zero reply-timeout failures (contracts A1, A2; SC-001, SC-002; red: some requests fail with `Reply timeout after Xms` despite every one being answered)

### Implementation for User Story 1

- [X] T039 [US1] Add `MessageAcked(matchId, sentTimestamp)` and `SendFailed(matchId, errorMessage)` to the sealed `TrackerMessage` trait in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala`, leaving `MessagePublished`'s field list byte-identical (research R5)
- [X] T040 [US1] Change `sentMessages`' value in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` to a private wrapper carrying `published`, `registeredAt`, `ackedAt` and `heldReply`; keep it a single-threaded `mutable.HashMap` — ordering, not concurrency, was the defect (data-model §1; research R3)
- [X] T041 [US1] Implement the two-phase completion in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala`: `MessageConsumed` holds the reply when `ackedAt` is absent, `MessageAcked` completes a held reply, and a response is always logged with `ackedAt` as its start (contracts T1, T2; invariants P2, P3)
- [X] T042 [US1] Change `TimeoutScan` in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` to measure `now - registeredAt` rather than from the ack (contract T3; invariant P4)
- [X] T043 [US1] Invert the call order in `sendKafkaMessage` in `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala`: `acquireTracker` → `tracker ! MessagePublished` → `sender.send`, with the ack callback sending `MessageAcked` and the error callback sending `SendFailed` (contracts A1, A4; this is the fix)
- [X] T044 [US1] In the same `acquireTracker` `onFailure` branch of `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala`, report the KO without publishing the record — same message text, same response-time span (contract A3; the change T034 approves)
- [X] T045 [US1] Confirm `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestAction.scala` (produce-only) is untouched by the diff (contract A6)
- [X] T046 [US1] Simplify `send` in `src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala` by removing the reply re-publish loop and its `#191, out of scope here` comment — a re-published reply masks a dropped one, so leaving it would hide the regression this story prevents (contracts §6)
- [X] T047 [US1] Tighten `KnownReplyLossBudget` to `0` in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaConcurrencyLoadTest.scala` and rewrite its scaladoc, which currently says "Tighten to 0 once #191 lands"
- [X] T048 [US1] Add a Migration Guide note under v1.1.0 in `README.md` recording that a failed tracker acquisition no longer publishes the request — this belongs in the #191 commit rather than a separate docs PR, because it documents that commit's own behaviour change (Principle I)
- [X] T049 [US1] Confirm five consecutive green runs of `sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"` at budget 0 before claiming SC-001, comparing against the T004 baseline
- [X] T050 [US1] Run `sbt scalafmtAll scalafmtSbt` then the default gate plus `ReplyRegistrationRaceSpec`; commit as `fix(request-reply): register the pending request before sending (#191)` with the milestone and `Closes #191`. State the slow-path send-thread risk from contracts §4 in the PR description

**Checkpoint**: All four stories complete.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Whole-feature verification and milestone closure. No production code changes here.

- [X] T051 [P] Run the API-compatibility gate: `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"` (Principle I; SC-010)
- [X] T052 Run the full CI gate against the Compose stack: `sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport`
- [X] T053 Walk `specs/003-request-reply-hardening/quickstart.md` end to end and correct anything that has drifted from the delivered code
- [X] T054 [P] Gate each PR with `scripts/check-linkage.sh --pr <N>` — milestone assigned, `Closes #NNN` present, issue in the same milestone (Principle V)
- [X] T055 Confirm milestone readiness with `scripts/check-linkage.sh --for-tag v1.1.0`; the milestone title `v1.1.0 Request-reply reliability` resolves for `--for-tag`
- [X] T056 [P] Comment on `https://github.com/galax-io/gatling-kafka-plugin/pull/144` noting that its register-before-send idea landed via #191 rather than #143, and close it if superseded (research R3 historical note)
- [X] T057 [P] Update `https://github.com/galax-io/gatling-kafka-plugin/issues/193`'s map: #143 and #166 are done, and point 3 (pool-owned correlation table) was **not** required for #191 — the ordering fix removed the need (research R3)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies
- **Foundational (Phase 2)**: depends on Setup. Pins baselines only — it blocks nothing structurally
- **User stories (Phases 3–6)**: each depends only on Phase 2. They are mutually independent
- **Polish (Phase 7)**: depends on every story intended for the release

### User Story Dependencies

None are hard. The phase order is a sequencing preference, and the two reasons for it are:

- **US4 before US1** — recommended by #193 so US1 is verified by a real echo responder in CI rather
  than by inference. US1 has its own `ReplyRegistrationRaceSpec`, so this is *soft*: US1 can ship
  first if needed, with weaker CI coverage until US4 lands.
- **US3 before US1** — both touch `KafkaMessageTracker.scala`. Landing US3 first means US1 rebases
  onto a correct tracker lifecycle instead of merging two changes to one file. A convenience, not a
  dependency.

### Within Each User Story

- Tests are written first and must FAIL before implementation (Principle IV)
- Contract/type additions before the logic that uses them
- Client-layer changes before action-layer changes
- Format, then full gate, then commit — one issue, one semantic commit, green on its own

### Parallel Opportunities

- T005–T007 (US2 tests) — three different assertions across two files
- T014–T015 (US4 broker definitions) — two different files
- T025–T027 (US3 tests) — two different files
- T035–T038 (US1 tests) — four independent assertions, one new file
- T051, T054, T056, T057 (Polish) — independent
- **Whole stories in parallel**: with more than one developer, US2/US4/US3/US1 can be taken
  concurrently. The only file collision is `KafkaMessageTracker.scala` between US3 and US1, and
  `KafkaMessageTrackerPool.scala` between US2 and US3

## Parallel Example: User Story 1

```bash
# All four US1 tests are independent — write them together, confirm all four go red:
Task: "Reply-before-ack join in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala"
Task: "TimeoutScan measures from registration in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala"
Task: "SendFailed releases the channel in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala"
Task: "Forced race in src/test/scala/org/galaxio/gatling/kafka/integration/ReplyRegistrationRaceSpec.scala"
```

## Implementation Strategy

### MVP: User Story 1 alone (#191)

US1 is the highest-value single slice: it is the only remaining source of *silent* data loss, and it
corrupts the report a load test exists to produce. It is independently shippable — Phase 6 needs
nothing from Phases 3–5, and `ReplyRegistrationRaceSpec` proves it without the echo responder.

If only one thing ships from this feature, ship US1. The recommended order still puts US4 first,
because a stronger CI oracle costs one test-only commit and makes US1's proof cheaper to trust.

### Recommended: all four, in phase order

1. Phase 1–2 → baselines pinned
2. Phase 3 (US2 / #143) → the terminal startup failure is gone
3. Phase 4 (US4 / #196) → the CI gate becomes an oracle instead of a coincidence
4. Phase 5 (US3 / #166) → the tracker lifecycle is correct
5. Phase 6 (US1 / #191) → no reply is ever dropped
6. Phase 7 → milestone `v1.1.0 Request-reply reliability` is tag-ready

### Parallel Team Strategy

Setup and Foundational are shared. After that, one developer per story works, with two coordination
points: `KafkaMessageTracker.scala` (US3 and US1) and `KafkaMessageTrackerPool.scala` (US2 and US3).
Sequence those two pairs or rebase; nothing else overlaps.

## Notes

- `[P]` = different files, no dependencies on incomplete tasks
- One issue = one semantic commit, green on its own under
  `sbt scalafmtCheckAll scalafmtSbtCheck compile test` (Principle V). Do not commit per task
- Every PR carries the `v1.1.0 Request-reply reliability` milestone and `Closes #NNN`
- Verify every test goes red before implementing it — for US1 and US2 the red condition must be
  *forced* (a parameterised initialization wait; a responder faster than the ack path), not waited for
- T034 is a genuine blocker on Phase 6, not a formality: it is a Principle I approval gate
