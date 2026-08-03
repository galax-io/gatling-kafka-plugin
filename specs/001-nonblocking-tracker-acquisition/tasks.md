# Tasks: Non-blocking Reply-Tracker Acquisition for Request-Reply Sends

**Input**: Design documents from `/specs/001-nonblocking-tracker-acquisition/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md),
[data-model.md](data-model.md), [contracts/internal-api.md](contracts/internal-api.md),
[quickstart.md](quickstart.md)

**Tests**: MANDATORY — this feature is a behavior change end-to-end (Constitution Principle IV).
Every test task below must be written first and observed to FAIL before its implementation task.
Kafka behavior is tested against Testcontainers, not mocks (Principle II).

**Organization**: Grouped by user story from spec.md. Contract guarantees G1–G14 refer to
[contracts/internal-api.md](contracts/internal-api.md).

**Commit mapping (Principle V)**: tasks ≠ commits. All implementation tasks converge into ONE
semantic commit `fix(client): keep tracker acquisition off the producer I/O thread (#163)` on top
of the `docs(speckit): add 001-nonblocking-tracker-acquisition spec/plan/tasks` commit. The PR
carries milestone `v1.1.0 Request-reply reliability` and `Closes #163`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)

## Path Conventions

Single-module Scala/sbt project. All paths below are real and repo-relative:

- Main: `src/main/scala/org/galaxio/gatling/kafka/{actions,client}/`
- Tests: `src/test/scala/org/galaxio/gatling/kafka/{client,actions,integration}/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Baseline sanity and the compatibility evidence the plan's Constitution gate cites.

- [X] T001 Verify clean baseline on branch `001-nonblocking-tracker-acquisition`: `sbt scalafmtCheckAll scalafmtSbtCheck compile test` green with Docker running (Testcontainers pulls images on first run)
- [X] T002 [P] Record the published-surface evidence for research.md R3: grep `src/main/java/ README.md src/test/scala/org/galaxio/gatling/kafka/examples/` for `addTopicForSubscription`, `tracker(`, `KafkaMessageTrackerPool`, `DynamicKafkaConsumer` and confirm the only call sites are `KafkaRequestReplyAction.scala`, protocol wiring, and the four test files named in plan.md; abort and re-plan if anything else appears

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Promise-based readiness in `DynamicKafkaConsumer` (contract G1–G5) — both US1 and
US2 build on it. The blocking `addTopicForSubscription` temporarily remains as a thin delegate so
the repo compiles at this checkpoint; it is deleted in US1 (T010) once the pool stops calling it.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T003 Write FAILING unit tests for `requestTopicSubscription` in src/test/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumerSpec.scala: (a) returns a future, queue holds `(topic, future)` (replaces latch-based test at lines 34–64); (b) returns an already-failed future after consumer failure without throwing — G2 (replaces throw-based test at lines 66–95); (c) `close()` fails still-pending futures exceptionally ("consumer closed") — G4
- [X] T004 [P] Migrate the three `addTopicForSubscription` call sites in src/test/scala/org/galaxio/gatling/kafka/integration/KafkaIntegrationSpec.scala (lines 245, 439–464) to `requestTopicSubscription(topic).get(timeout)`-style waits on the test thread; the "very short timeout returns false" test becomes "very short `get` timeout throws `java.util.concurrent.TimeoutException` while the future stays pending" (does not compile until T005 — that is the red state)
- [X] T005 Implement promise-based readiness in src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala: `topicsQueue` element type `(String, CompletableFuture[Void])`; new `requestTopicSubscription(topic): CompletableFuture[Void]` (fail-fast already-failed future per G2, enqueue, poke `initLatch`, never block — G1); `updateSubscription` already-subscribed branch and `onPartitionsAssigned` complete futures (was `countDown`); `markConsumerFailed` completes queued futures exceptionally with the failure cause; `run()` finally-block and `close()` drain remaining queue entries exceptionally ("consumer closed") — G4; keep `addTopicForSubscription(topic, timeout): Boolean` as a temporary thin blocking delegate over the future preserving today's return/throw semantics
- [X] T006 Green gate for Phase 2: `sbt "testOnly org.galaxio.gatling.kafka.client.DynamicKafkaConsumerSpec"` and `sbt "testOnly org.galaxio.gatling.kafka.integration.KafkaIntegrationSpec"` pass; full `sbt compile test` still green

**Checkpoint**: Readiness is future-based and fully covered; consumer never leaves a pending
future unresolved (assignment / already-subscribed / failure / close all resolve it).

---

## Phase 3: User Story 1 — Slow reply-channel setup on one topic must not stall other traffic (Priority: P1) 🎯 MVP

**Goal**: The producer delivery callback never waits on tracker acquisition; a topic whose
readiness hangs affects only its own requests (spec US1, FR-001/FR-002/FR-007, SC-001/SC-002/SC-003).

**Independent Test**: `sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerAcquisitionIsolationSpec"`
— red before T008–T012, green after.

### Tests for User Story 1 (write FIRST, observe FAIL) ⚠️

- [X] T007 [US1] Create src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala with its own Testcontainers Kafka container configured `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (research R4) and a harness mirroring KafkaIntegrationSpec (real `KafkaSender`, `KafkaMessageTrackerPool`, short ~3 s reply timeout, pre-created healthy topic pair, nonexistent "poisoned" reply topic). Three FAILING tests: (1) fire poisoned-topic request-reply then healthy-topic request-reply through the shared producer — healthy completes in ≪ reply timeout (SC-001; red today: it queues behind the parked callback thread for the full timeout); (2) while the poisoned preparation is pending, additional healthy sends get delivery confirmations promptly (SC-002/FR-002); (3) N concurrent first-use requests against one new (pre-created) topic all proceed once assigned, with a single subscription request and no callback-thread stall (FR-007). Run the spec and record the red failures

### Implementation for User Story 1

- [X] T008 [US1] Implement `acquireTracker` in src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala per contract G6–G10: add single-thread daemon `ScheduledThreadPoolExecutor` field (thread name `gatling-kafka-tracker-setup`), shut down in the existing `registerOnTermination` block; fast path = existing `computeIfPresent` refcount++ then `onReady` inline; poisoned-pool check reports via `onFailure` synchronously (G7); slow path = `consumer.requestTopicSubscription(topic)` + scheduled timeout task completing the future exceptionally with `RuntimeException("Timed out waiting for consumer assignment to topic '<topic>' after <timeout>")` (G8, cancelled if readiness wins) + `whenCompleteAsync` on the setup executor running the existing insert-or-increment `trackers.compute` then `onReady(actor)` / `onFailure(cause)` (G9); delete the blocking `tracker(...)` method
- [X] T009 [US1] Rewire src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala per G11–G14: success callback keeps only debug log + `requestMatch` + `acquireTracker(...)` call; `onReady` records `sentTimestamp = clock.nowMillis` and sends `MessagePublished(..., onComplete = releaseTracker(...))` (G12); `onFailure` reuses the existing KO reporting extracted into a private helper shared with the send-failure branch (G13); remove the `trackerAcquired` try/catch-release block (pool owns registration consistency); send-failure branch stays byte-for-byte (G14/FR-006)
- [X] T010 [US1] Delete the temporary `addTopicForSubscription` delegate from src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala (no callers remain after T004/T008) and verify with `grep -rn addTopicForSubscription src/` → zero hits
- [X] T011 [P] [US1] Migrate the `pool.tracker(...)` call site (line 36) in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPoolSpec.scala to `acquireTracker` with a small await-both-callbacks test helper; assertions unchanged
- [X] T012 [P] [US1] Migrate acquire/release call sites in src/test/scala/org/galaxio/gatling/kafka/client/TrackerRefCountSpec.scala to the same helper; all existing refcount/release invariants must pass unchanged (G9; guards FR-006's no-residual-reservation property)
- [X] T013 [US1] Green gate for US1: TrackerAcquisitionIsolationSpec tests 1–3 pass; full `sbt compile test` green

**Checkpoint**: The defect in #163 is neutralized — MVP. Delivery callbacks are non-blocking;
cross-topic isolation proven against a real broker.

---

## Phase 4: User Story 2 — Reply-channel setup failure affects only the requesting operation (Priority: P2)

**Goal**: Preparation timeout/failure KOs exactly the affected request with a descriptive error;
the pool stays usable and the topic retryable (spec US2, FR-003, SC-004).

**Independent Test**: the failure-semantics tests below run standalone via
`sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerAcquisitionIsolationSpec org.galaxio.gatling.kafka.client.KafkaMessageTrackerPoolSpec"`.

### Tests for User Story 2 (write FIRST, observe FAIL) ⚠️

- [X] T014 [P] [US2] Add FAILING integration tests to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala: (4) poisoned-topic request is reported KO at ≈ the reply timeout with an error message naming the topic and the timeout, and the virtual-user chain continues (SC-004); (5) after that KO — a healthy-topic request still completes OK, and a fresh poisoned-topic request attempts preparation again, KO-ing after its own full timeout rather than instantly (FR-003, retry-not-poisoned)
- [X] T015 [P] [US2] Add FAILING unit test to src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPoolSpec.scala: after the pool's consumer-failure callback has fired, `acquireTracker` invokes `onFailure` synchronously on the calling thread with the existing "Kafka consumer failed; tracker pool can no longer be used" cause — and never invokes `onReady` (G7)
- [X] T016 [US2] Close any gaps the red tests expose in src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala and src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala (expected near-no-op if T008/T009 followed the contract: exact timeout message text, timeout-task cancellation on success, KO timing spanning request start → failure detection); green gate: US2 tests + full `sbt test`

**Checkpoint**: Failure isolation proven — one misconfigured topic produces attributed KOs only.

---

## Phase 5: User Story 3 — Reported latency reflects the system under test, not plugin setup (Priority: P3)

**Goal**: Successful first-use requests report round-trip time excluding readiness preparation;
failed requests keep request-start → failure-detection timing (spec US3, FR-005).

**Independent Test**: latency-semantics test runs standalone inside
TrackerAcquisitionIsolationSpec; CI Gatling simulations corroborate at the report level.

### Tests for User Story 3 (write FIRST, observe FAIL if semantics drift) ⚠️

- [X] T017 [US3] Add integration test (6) to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala using the recording-StatsEngine pattern from src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessagesSpec.scala: initiate a request-reply whose reply topic does not exist yet, create the topic via AdminClient after ~2 s (readiness then completes), have the responder reply immediately; assert the logged OK response duration is ≪ the 2 s readiness delay (sent timestamp recorded at tracking-ready instant — FR-005), and assert the KO case from T014 logs duration ≈ request-start → failure-detection. This test must FAIL if `sentTimestamp` is ever recorded at ack time or send-initiation time instead of `onReady`
- [X] T018 [US3] Verify `sentTimestamp` placement in src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala matches G12 exactly (recorded inside `onReady`, immediately before `MessagePublished`); adjust if T017 is red; green gate: full `sbt test`

**Checkpoint**: All three stories independently verified.

---

## Phase 6: Polish & Cross-Cutting Verification

**Purpose**: Repo-wide gates from quickstart.md (SC-005, FR-008) and contract conformance.

- [X] T019 Format: `sbt scalafmtAll scalafmtSbt`
- [X] T020 Full local gate green: `sbt scalafmtCheckAll scalafmtSbtCheck compile test`
- [X] T021 [P] API-compat witness (Principle I): `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"` passes unchanged
- [X] T022 CI-equivalent Gatling run against the Compose stack (`docker compose -f docker-compose.kafka.yml up -d` first): `sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport` — request-reply timings in the report in line with current release (US3 witness)
- [X] T023 [P] Contract conformance sweep: `grep -rn "addTopicForSubscription\|def tracker(" src/main/` → zero hits (no blocking acquisition API survives); review the three changed main files against the thread-role table in contracts/internal-api.md (no `get`/`await`/`join` on producer-callback or consumer-thread paths)
- [X] T024 Execute [quickstart.md](quickstart.md) top to bottom and confirm every assertion-to-spec mapping row holds; then assemble the single `fix(client): keep tracker acquisition off the producer I/O thread (#163)` commit on top of the `docs(speckit)` commit, PR with milestone `v1.1.0 Request-reply reliability` + `Closes #163` (gate: `scripts/check-linkage.sh --pr <N>`)

---

## Dependencies & Execution Order

### Phase Dependencies

```text
Setup (T001–T002)
  └─▶ Foundational (T003–T006)  ← consumer readiness futures; BLOCKS all stories
        └─▶ US1 (T007–T013)     ← pool + action + isolation proof; MVP
              ├─▶ US2 (T014–T016)  ← failure semantics on US1 machinery
              └─▶ US3 (T017–T018)  ← measurement semantics on US1 machinery
                    └─▶ Polish (T019–T024)
```

- **US2 and US3 both depend on US1** (they assert semantics of the machinery US1 builds) but are
  independent of each other; run sequentially P2 → P3 by default because both edit
  `TrackerAcquisitionIsolationSpec.scala` (coordinate if parallelizing).
- Within every story: red test task strictly before its implementation task (Principle IV).

### Parallel Opportunities

- Phase 1: T002 alongside T001.
- Phase 2: T003 ∥ T004 (different test files) before T005.
- US1: T011 ∥ T012 (different test files) after T008; T009 after T008 (uses the new signature);
  T010 after T004+T008.
- US2: T014 ∥ T015 (different files) before T016.
- Polish: T021 ∥ T023 after T020.

## Parallel Example: User Story 1

```bash
# After T008 lands, run the two test migrations in parallel (different files):
Task: "Migrate pool.tracker call site in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPoolSpec.scala"
Task: "Migrate acquire/release sites in src/test/scala/org/galaxio/gatling/kafka/client/TrackerRefCountSpec.scala"
```

## Implementation Strategy

**MVP = Setup + Foundational + US1 (T001–T013)**: after T013 the producer I/O thread can no
longer be parked by tracker acquisition — the reported defect is gone and independently proven.
US2 then locks failure attribution, US3 locks measurement semantics; both are thin verification
layers over the same machinery. Stop-and-validate is possible at every checkpoint; the feature
still ships as one semantic commit at the end (Principle V — tasks are steps, not commits).

## Execution record (deviations from the plan as written)

- **T005/T008 landed together; no temporary `addTopicForSubscription` delegate.** The delegate
  existed only to keep Phase 2 compiling on its own; adding and deleting it inside one PR is the
  add-then-remove churn Principle V forbids. Both phases were verified at one checkpoint instead.
- **T012 required no migration.** `TrackerRefCountSpec` mirrors the `ConcurrentHashMap` refcount
  algorithm locally and never calls the pool, so it guards the unchanged `registerTracker` /
  `releaseTracker` logic as-is (10/10 green throughout).
- **Red-first verification method.** The isolation spec cannot fail against the pre-fix *API* (it
  would not compile), so the pre-fix *behaviour* was restored instead by making `acquireTracker`
  wait inline on the readiness future. 4 of 6 tests went red with the intended diagnostics
  (`delivery callback was held for 5252 ms waiting for the assignment`; `unrelated traffic took
  5039 ms to get through while one reply topic was being assigned`), then green once the
  asynchronous version was restored. Tests 4 and 5 cover failure attribution and pass either way,
  by design.
- **The isolation spec induces the stall with `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`, not with an
  unassignable topic.** Subscribing to a nonexistent topic still fires `onPartitionsAssigned`, so
  it produces no stall. A broker-side initial rebalance delay is a genuine slow assignment and needs
  no `auto.create.topics.enable=false`.
- **Two defects surfaced during T022 that the plan had not predicted** — the readiness-in-listener
  strand and the test broker's 3 s rebalance delay. Both are written up in
  [plan.md](plan.md#discovered-during-implementation-design-deltas-from-the-original-outline);
  the second required the only file change outside the planned set,
  `docker-compose.kafka.yml`.

## Notes

- Verify each red test actually fails before implementing — record the failure in the task log.
- No new dependencies at any point (JDK `java.util.concurrent` only) — Constitution constraints.
- Sibling issues #143/#164/#165/#166 stay untouched even where adjacent code invites fixes
  (Boundaries: no opportunistic refactors; `releaseTracker` and `updateSubscription` churn logic
  are modified by their own issues, not here).
