---

description: "Task list for 004-reply-correlation-correctness"
---

# Tasks: Reply Correlation Correctness

**Input**: Design documents from `/specs/004-reply-correlation-correctness/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/behavior-contract.md](./contracts/behavior-contract.md)

**Tests**: MANDATORY per Constitution Principle IV. Every task below that changes observable behaviour
has a test task before it, and each must be demonstrated failing against pre-change code (FR-025).
Per Principle II, Kafka behaviour is tested against Testcontainers or the `docker-compose.kafka.yml`
stack — never mocks.

**Organization**: One phase per user story, in spec priority order.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel — different files, no dependency on an incomplete task
- **[Story]**: US1–US4 from [spec.md](./spec.md)

## Path Conventions

Single-module Scala/sbt project:

- **Plugin sources**: `src/main/scala/org/galaxio/gatling/kafka/{protocol,actions,client,checks,request}/`
- **Tests**: `src/test/scala/org/galaxio/gatling/kafka/{client,checks,actions,integration,examples}/`
- **Broker/CI truth**: `docker-compose.kafka.yml`, `.github/workflows/ci.yml`

---

## Scope & Effort

Production change is **~50 lines across 6 files**. Verification is roughly 70% of the work — 3 new test
files, 3 amended simulations, 2 broker definitions, 2 CI edits.

| Story | Issue | Production change | Value |
|---|---|---|---|
| US1 | #167 (p0) | 2 lines + a guard | Replies stop being credited to the wrong virtual user |
| US2 | #168 (p1) | ~20 lines | Virtual users stop disappearing on an empty reply |
| US3 | #193 (p1) | ~25 lines | False timeouts at channel start disappear |
| US4 | #167 (p0) | none — falls out of US1 | Keyless traffic stops collapsing onto one partition |

**T014 is the single highest-value line in this list.** It wires the already-written
`KafkaConcurrencyLoadTest` into CI. Today every request-reply scenario in CI runs `atOnceUsers(1)`, so
concurrency defects cannot be caught at all — for this feature or any future one. Do it early in
Phase 3.

**If scope must be cut**, drop from the bottom: US3's evidence of real-world impact is the weakest
(research §R4 — the project's own simulations already pass without the broker workaround). US1+US2+US4
is a coherent release that closes three quarters of the milestone.

---

## Commit & Issue Mapping (Principle V)

One tracked issue = one semantic commit, green on its own. Spec artifacts land first, separately.

| Commit | Issues | Phases | Milestone |
|---|---|---|---|
| `docs(speckit): add 004-reply-correlation-correctness spec/plan/tasks` | — | Phase 1 | v1.2.0 |
| `fix(actions): correlate keyless request-reply per request (#167)` | Closes #167 | Phase 3 + Phase 6 | v1.2.0 |
| `fix(checks): fail cleanly on replies with no payload (#168)` | Closes #168 | Phase 4 | v1.2.0 |
| `feat(client): complete reply readiness only once positioned (#193)` | Closes #193 | Phase 5 | v1.2.0 |

**US1 and US4 share issue #167** — both come from the same `key.orNull` change. Separate phases because
they are independently testable; one commit because they are one fix.

**Migration Guide entries ride with the commit whose behaviour they describe** (FR-017), not in a
separate docs PR. This is the one deliberate departure from "docs go in their own PR", justified in
[plan.md](./plan.md) Constitution Check §V.

---

## Phase 1: Setup

**Purpose**: Land the spec, and establish the baseline that "fails before" is measured against

- [X] T001 Commit every artifact under `specs/004-reply-correlation-correctness/` as one `docs(speckit):` commit, before any `fix`/`feat` commit (Principle V, spec-first) — landed as `72fde73`
- [X] T002 Confirm the pre-change baseline is green by running `sbt scalafmtCheckAll scalafmtSbtCheck compile test` at the repository root — 57 passed, 0 failed
- [X] T003 Start the broker stack with `docker compose -f docker-compose.kafka.yml up -d` and confirm `KafkaGatlingTest` and `KafkaJavaapiMethodsGatlingTest` pass unchanged — green on re-run; **first run after an idle broker failed with 2 extra reply timeouts**, see research §R9

**Checkpoint**: A known-green baseline exists, so any red produced later is attributable to a new test.

---

## Phase 2: Foundational

**Purpose**: Blocking prerequisites for all stories

**There are none.** The three defects live in different layers — `actions/` + `client/` for #167,
`checks/` for #168, `client/` for #193 — and no story depends on another's production change. Every
story is unblocked after Phase 1.

What needs coordination is **shared files**: five are touched by more than one story and must not be
edited in parallel. See [Shared-file constraints](#shared-file-constraints-).

**Checkpoint**: All four stories may proceed, in priority order or in parallel.

---

## Phase 3: User Story 1 — A reply is always reported against the user that sent the request (P1) 🎯 MVP

**Goal**: Keyless request-reply requests stop sharing one correlation slot. Where correlation is
possible, each reply reaches its own virtual user; where it is not, the request fails at issue time with
a reason naming the missing identity.

**Independent Test**: Concurrent keyless request-reply users correlating on a non-key field against the
echo responder — request count equals success count, zero timeouts, zero cross-attribution. Separately,
the same scenario correlating on the key — every request KOs at issue time.

### Tests for User Story 1 (MANDATORY — write first, confirm they FAIL) ⚠️

- [X] T004 [P] [US1] Unit test that `null` and `Array.emptyByteArray` produce distinct match keys, so a reply with an empty id cannot resolve a request registered with none. Two *null* registrations are not covered here and cannot be: the tracker now refuses to register a null match id at all (`Arrays.equals(null, null)` is true, so they would alias). That refusal is asserted separately. In `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala`
- [X] T005 [P] [US1] Unit test that a request-reply whose matcher yields `null` is reported KO at issue time with a message naming the missing identity, and is never handed to the sender, in `src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessagesSpec.scala`
- [X] T006 [P] [US1] New Testcontainers spec `KeylessCorrelationSpec` in `src/test/scala/org/galaxio/gatling/kafka/integration/KeylessCorrelationSpec.scala`, with two tests modelled on `ReplyRegistrationRaceSpec`:
  - **(a) the red-before gate** — N concurrent keyless exchanges under `KafkaKeyMatcher`: every request must be KO'd with the missing-identity message, none sent, none matched. Pre-fix this fails because the requests collapse into one slot, producing a mix of wrongly-matched OKs and "match id reused" KOs.
  - **(b) a guard on the fix** — N concurrent keyless exchanges under `KafkaValueMatcher` with distinct values: every request matched to its own reply, zero timeouts. This passes before *and* after; it exists so making the key `null` cannot silently break correlation that already worked.
- [X] T007 [US1] Red-before demonstrated. Unit: 3 failed / 21 passed — absent-vs-empty match id resolved wrongly (got 1 match, expected 0); no-correlation-id reported no outcome at all (got 0, expected 1). Integration (production files reverted to HEAD): gate test (a) FAILED at 33 s with 0 of 24 requests rejected — all were registered and sent; guard test (b) passed, as expected for a matcher that never reads the key. After the fix: 24/24 unit, 2/2 integration.

### Implementation for User Story 1

- [X] T008 [P] [US1] Replace `key.getOrElse(Array.emptyByteArray)` with `key.orNull` in `resolveToProtocolMessage` in `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaAction.scala` (data-model §1; also delivers US4)
- [X] T009 [P] [US1] Remove the `if (m == null) Array.emptyByteArray else m` substitution from `matchKeyFor` in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` so `null` and empty become distinct `MatchKey`s (data-model §2, VR-2)
- [X] T010 [US1] After `val id = matcher.requestMatch(protocolMessage)` in `sendKafkaMessage`, report KO via the existing `reportFailure(...)` and return when `id == null` — before `acquireTracker` and before `sender.send` — in `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala` (research §R2, VR-1)
- [X] T011 [P] [US1] Document in scaladoc that `key` and `value` are nullable and that absent differs from empty, in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaProtocolMessage.scala`
- [X] T012 [US1] Audit every read of `KafkaProtocolMessage.key` under `src/main/scala/org/galaxio/gatling/kafka/` for a non-null assumption now that the produce side can carry `null` (`describeBytes` in `package.scala` already handles it; confirm the rest)

### Simulation-level verification for User Story 1 (FR-018, FR-019)

- [X] T013 [US1] Add a keyless request-reply scenario correlating on value with **concurrent** user injection, plus a keyless scenario correlating on the key whose requests must all KO, using exact-count `is(...)` assertions and a named `details(...)` per expected failure, in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` (FR-024)
- [X] T014 [US1] Add `"Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"` to the existing Gatling step in `.github/workflows/ci.yml` and correct its scaladoc header in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaConcurrencyLoadTest.scala`, which currently says "Not wired into CI" (research §R8, FR-019 — **do this first in the phase**)
- [X] T015 [US1] Add a Migration Guide entry for contract C1 — keyless request-reply under key matching now fails, with both remediations (set a key, or `matchByValue`/`matchByMessage`) — in `README.md`

**Checkpoint**: US1 complete and independently verifiable. This is the MVP — the defect that makes the
report actively wrong rather than merely incomplete.

---

## Phase 4: User Story 2 — A reply with no payload fails cleanly instead of stopping the user (P2)

**Goal**: A reply carrying no payload produces a readable KO and the virtual user continues. No check,
of any kind, can leave a request without a terminal outcome.

**Independent Test**: A request-reply scenario with a reply-content check against a responder answering
with an absent payload — every request KOs with a stated reason, and every virtual user reaches the end
of its scenario.

### Tests for User Story 2 (MANDATORY — write first, confirm they FAIL) ⚠️

- [ ] T016 [P] [US2] New unit spec `KafkaMessagePreparerSpec` covering all five preparers against absent, empty and present payloads — absent yields a `Validation` failure naming the cause, empty keeps today's behaviour (`""` / empty bytes, success), present parses — in `src/test/scala/org/galaxio/gatling/kafka/checks/KafkaMessagePreparerSpec.scala` (VR-5 – VR-7)
- [ ] T017 [P] [US2] Unit test that a check which throws still produces a terminal KO and still continues the virtual user, in `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala` (VR-8)
- [ ] T018 [US2] Run `sbt "testOnly org.galaxio.gatling.kafka.checks.KafkaMessagePreparerSpec org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"` against unmodified code and record that T016 fails with an NPE and T017 fails with no continuation

### Implementation for User Story 2

- [ ] T019 [US2] Null-guard `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer` behind `safely(...)`, matching the existing shape of `xmlPreparer`/`avroPreparer` in the same file, in `src/main/scala/org/galaxio/gatling/kafka/checks/KafkaMessagePreparer.scala` (research §R6)
- [ ] T020 [US2] Add a `catch` to the `try`/`finally` in `completeMatched` that reports KO and continues the virtual user, in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala` (VR-8; **same file as T009 — sequence after Phase 3**)

### Simulation-level verification for User Story 2 (FR-021)

- [ ] T021 [US2] Add a request-reply scenario answered with an absent payload and carrying a reply-content check, asserting **both** the exact KO count and that a request executed after it in the same scenario also ran — a stalled user produces no failure, so a failure count alone goes green on a hung run — in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`
- [ ] T022 [US2] Add a Migration Guide entry for contract C3 — an absent reply payload now KOs instead of stalling the user, and `bodyString.is("")` does not pass on a tombstone — in `README.md`

**Checkpoint**: US1 and US2 both independently functional.

---

## Phase 5: User Story 3 — A reported timeout always means the system did not answer (P3)

**Goal**: A reply channel reports ready only once it can actually receive, so a reply published from
that moment on is never skipped and never becomes a false timeout.

**Independent Test**: `PositionedReadinessSpec` — with a continuous numbered producer stream running,
assert `F <= S`, where `S` is the producer sequence at the moment readiness completed and `F` is the
first record delivered. One run, decisive in both directions.

### Tests for User Story 3 (MANDATORY — write first, confirm they FAIL) ⚠️

- [ ] T023 [US3] New Testcontainers spec `PositionedReadinessSpec` — fresh topic and consumer group with `auto.offset.reset=latest`, a continuous numbered producer stream started **before** the subscription request, capture `S` when readiness completes and `F` from the first delivered record, assert `F <= S` and report `F - S` on failure — in `src/test/scala/org/galaxio/gatling/kafka/integration/PositionedReadinessSpec.scala` (research §R5)
- [ ] T024 [US3] Run `sbt "testOnly org.galaxio.gatling.kafka.integration.PositionedReadinessSpec"` against unmodified code and record `F > S` with the observed gap size; if it passes pre-change the stream was idle at readiness, so fix `src/test/scala/org/galaxio/gatling/kafka/integration/PositionedReadinessSpec.scala` before concluding the defect is absent

### Implementation for User Story 3

- [ ] T025 [US3] In `completeAssignedReadiness`, resolve `consumer.position(tp, timeout)` for every assigned partition of a topic awaiting readiness **before** completing that topic's futures, in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala` (research §R3, VR-9)
- [ ] T026 [US3] On position timeout or failure, complete **only that topic's** readiness futures exceptionally and do not call `markConsumerFailed`, which would poison the pool for the rest of the run — the #143 terminal state — in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala` (VR-10)
- [ ] T027 [US3] Confirm both call sites remain poll-thread-only (`onPartitionsAssigned` and the tail of `updateSubscription()`) and update `completeAssignedReadiness`'s scaladoc to state that readiness now means positioned rather than merely assigned, in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala` (VR-11)

### Broker-definition cleanup for User Story 3 (FR-022)

> Sequence **after** T025–T026. Research §R4 found this removal does **not** turn CI red pre-fix —
> `docker-compose.kafka.yml:25-33` records that the simulations were already verified green at Kafka's
> 3000 default. It is hygiene, not the gate. The gate is T023.

- [ ] T028 [US3] Remove `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` and its explanatory comment from `docker-compose.kafka.yml`
- [ ] T029 [US3] Remove `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` from `.github/workflows/ci.yml`
- [ ] T030 [US3] Re-run the Compose-stack simulation suite per [quickstart.md](./quickstart.md) step 3 with the setting gone and confirm green (SC-010)

**Checkpoint**: US1, US2 and US3 all independently functional.

---

## Phase 6: User Story 4 — Keyless traffic spreads across partitions (P4)

**Goal**: Keyless messages are distributed across a topic's partitions the way any ordinary producer
distributes them, so a keyless throughput run measures the partitioned system it was meant to measure.

**Independent Test**: Publish many keyless messages to a multi-partition topic and read back where they
landed — every partition receives messages.

> **No production change of its own.** This falls out of T008 (`key.orNull`) and ships in the #167
> commit with Phase 3.

### Tests for User Story 4 (MANDATORY — write first, confirm they FAIL) ⚠️

- [X] T031 [US4] Add an explicitly multi-partition topic to the `topic-init` service in `docker-compose.kafka.yml` — every existing topic there is `--partitions 1` and `KAFKA_AUTO_CREATE_TOPICS_ENABLE` would otherwise auto-create at 1 partition, leaving nothing for the defect to be visible against (research §R7). **Same file as T028 — sequence after it**
- [X] T032 [US4] Add a scenario publishing keyless messages to that topic with an assertion that reads placement back directly (per-partition end offsets, or consume and group by partition) and requires every partition to have received messages — the defective behaviour raises no error, so absence of failure proves nothing — in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`
- [X] T033 [US4] Run `sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"` against unmodified code and record that T032's messages all land on a single partition

### Verification for User Story 4

- [X] T034 [US4] Confirm keyed messages still land by `hash(key) % n` by asserting placement for an existing keyed scenario in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala` (contract C4, FR-015)
- [X] T035 [US4] Add a Migration Guide entry for contract C4 — keyless messages now spread across partitions, so keyless throughput numbers may move because they previously described a single-partition workload — in `README.md`

**Checkpoint**: All four stories independently functional.

---

## Phase 7: Polish & Cross-Cutting

- [ ] T036 Run `sbt scalafmtAll scalafmtSbt`, then confirm `sbt scalafmtCheckAll scalafmtSbtCheck compile test` is green at the repository root
- [ ] T037 Run `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"` — the Principle I gate; a failure here is an API break to reconsider, not a check to relax (SC-008)
- [ ] T038 Run the full CI Gatling line including `KafkaConcurrencyLoadTest` per [quickstart.md](./quickstart.md) step 3
- [ ] T039 Walk the red-before/green-after table in [quickstart.md](./quickstart.md) and confirm all four stories are demonstrated in both directions (SC-009)
- [ ] T040 Verify the Migration Guide section of `README.md` covers contracts C1, C3 and C4 with remediation for each (FR-017)
- [ ] T041 Split the work into the three semantic commits in [Commit & Issue Mapping](#commit--issue-mapping-principle-v), each green on its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`
- [ ] T042 Open one PR per issue, each assigned to milestone `v1.2.0 Reply correlation correctness` with `Closes #NNN`, and verify each with `scripts/check-linkage.sh --pr <N>`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: no dependencies
- **Phase 2 (Foundational)**: empty — nothing blocks the stories
- **Phases 3–6 (Stories)**: depend only on Phase 1. Priority order is P1 → P2 → P3 → P4, but the
  production changes are independent
- **Phase 7 (Polish)**: depends on every story intended for the release

### Shared-file constraints ⚠️

Five files are touched by more than one story. These tasks **must not** run in parallel:

| File | Tasks | Order |
|---|---|---|
| `client/KafkaMessageTracker.scala` | T009 (US1), T020 (US2) | T009 → T020 |
| `examples/KafkaGatlingTest.scala` | T013 (US1), T021 (US2), T032 + T034 (US4) | T013 → T021 → T032 → T034 |
| `docker-compose.kafka.yml` | T028 (US3), T031 (US4) | T028 → T031 |
| `.github/workflows/ci.yml` | T014 (US1), T029 (US3) | T014 → T029 |
| `README.md` | T015 (US1), T022 (US2), T035 (US4) | T015 → T022 → T035 |

Because these span commits, apply each edit inside the commit that owns it — do not batch the file.

### Ordering constraints within stories

- **All tests before their implementation**, each demonstrated failing (T007, T018, T024, T033)
- **T008 before T032** — US4's assertion cannot pass until `key.orNull` lands
- **T025–T026 before T028–T029** — fix the readiness gap before removing the broker workaround
- **T010 after T009** — fail-fast reads the identity `matchKeyFor` must already treat correctly

### Parallel Opportunities

- T004, T005, T006 — three different test files, fully parallel
- T008, T009, T011 — three different production files, fully parallel
- T016, T017 — different test files, parallel
- Across stories: US1 and US3 share no files at all and can be built simultaneously by two people
- US2 and US4 each collide with US1 on at least one file (see table), so they trail it

---

## Parallel Example: User Story 1

```bash
# Write all three failing tests together:
Task: "Unit test null vs empty match keys in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala"
Task: "Unit test keyless KO at issue time in src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessagesSpec.scala"
Task: "Testcontainers KeylessCorrelationSpec in src/test/scala/org/galaxio/gatling/kafka/integration/KeylessCorrelationSpec.scala"

# Then the three independent production edits together:
Task: "key.orNull in src/main/scala/org/galaxio/gatling/kafka/actions/KafkaAction.scala"
Task: "Drop the null substitution in matchKeyFor in src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala"
Task: "Document key/value nullability in src/main/scala/org/galaxio/gatling/kafka/request/KafkaProtocolMessage.scala"
```

---

## Implementation Strategy

### MVP — User Story 1 only

1. Phase 1 (Setup)
2. Phase 3 (US1), starting with T014 — one line, and it is what makes every later concurrency claim
   checkable
3. **STOP and VALIDATE**: concurrent keyless correlation correct, keyless key-matched requests KO at
   issue time
4. Ships as the `fix(actions): … (#167)` commit

US1 alone is a defensible release: it removes the failure that misattributes results between virtual
users, which is the one that makes a report actively wrong.

### Incremental delivery

1. Setup → baseline green
2. **US1 + US4** → one commit, `Closes #167` → the report stops lying about who owns a reply
3. **US2** → one commit, `Closes #168` → no virtual user can be lost to a check
4. **US3** → one commit, `Closes #193` → a timeout means the system did not answer
5. Polish → three PRs, one milestone, release-ready

### Parallel team strategy

- Developer A: US1 then US4 — same commit, same files
- Developer B: US3 — zero file overlap with US1, fully independent
- Developer C: US2 — waits on T009 for `KafkaMessageTracker.scala`, otherwise independent

---

## Notes

- `[P]` = different files, no dependency on an incomplete task
- Verify every test fails before implementing it — FR-025 requires it demonstrated, not assumed
- Commit per issue, not per task (Principle V) — three commits after the spec commit
- Only contracts C1, C3 and C4 need Migration Guide entries; C5 is strictly an improvement
- Do not treat removing `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` as evidence the readiness gap is
  closed — research §R4 explains why it stays green either way
