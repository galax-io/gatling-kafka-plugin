---

description: "Task list for 008-measurement-truth"
---

# Tasks: Measurement Truth in Request-Reply

**Input**: Design documents from `/specs/008-measurement-truth/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/behavior-contract.md](./contracts/behavior-contract.md), [quickstart.md](./quickstart.md)

**Milestone**: [v2.1.0 Measurement truth](https://github.com/galax-io/gatling-kafka-plugin/milestone/16) — [#227](https://github.com/galax-io/gatling-kafka-plugin/issues/227) (US1), [#228](https://github.com/galax-io/gatling-kafka-plugin/issues/228) (US2)

**Tests**: Mandatory. Every task below that changes observable behaviour has a test written to fail
before it (Constitution IV), and every Kafka interaction is exercised against a real broker —
Testcontainers or the `docker-compose.kafka.yml` stack (Constitution II). Two of the paths this
feature touches have **no test at all today**: the missing-consumer rejection and how the
acquisition failure is reported. Their tests are written first and are new coverage, not edits.

**Organization**: By user story. US1 and US2 touch disjoint source files and are independently
deliverable. The one file both edit is `KafkaFailureModesGatlingTest.scala`, which is why T003
restructures its pinned counts before either story adds to them.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1 = #227, US2 = #228, US3 = the Migration Guide these two owe
- Contract clauses `C1`…`C12` are from [contracts/behavior-contract.md](./contracts/behavior-contract.md)

## Path Conventions

Single-module Scala/sbt project:

- **Plugin sources**: `src/main/scala/org/galaxio/gatling/kafka/{actions,client,protocol,checks,request}/`
- **Java facade**: `src/main/java/org/galaxio/gatling/kafka/javaapi/`
- **Tests**: `src/test/scala/`
- **Example consumer projects**: `examples/{scala,java,kotlin}/`

---

## Phase 1: Setup

**Purpose**: A broker to test against, and a recorded baseline so every later red is attributable to
a change rather than to the environment.

- [X] T001 Start the broker stack with `docker compose -f docker-compose.kafka.yml up -d` and confirm Kafka, Zookeeper and Schema Registry are reachable, per [quickstart.md](./quickstart.md)
- [X] T002 Record the baseline: run `sbt scalafmtCheckAll scalafmtSbtCheck compile test` and `sbt "Gatling / test"`, and save the four harnesses' pass state and the exact counts `KafkaFailureModesGatlingTest` currently asserts (`ExpectedFailures`, `ExpectedRequests`, and each `details(...)`) from `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The one thing both stories share. Thin by design — US1 and US2 are otherwise
independent, and inventing shared scaffolding they do not need would couple them.

**⚠️ CRITICAL**: T003 must land before either story edits the failure-modes harness.

- [X] T003 Restructure the pinned expected-count constants in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala` so each scenario contributes a separately named constant to `ExpectedFailures` and `ExpectedRequests`, instead of both being one arithmetic expression. Counts stay pinned with `is(n)` in both directions — the harness's own policy is that an expected failure which starts passing must fail the run too
- [X] T004 [P] Confirm `src/test/scala/io/gatling/core/stats/RecordingStatsEngine.scala` records everything clauses C1, C2, C9 and C10 assert on — request name, status, start and end instants, message — and note in a comment that its dropping of `responseCode` and no-op `logRequestCrash` match what Gatling itself keeps (research.md C3, C4). Extend it only if a clause cannot be expressed

**Checkpoint**: The harness can absorb new scenarios without either story fighting the other's counts.

---

## Phase 3: User Story 1 - A latency number describes only requests that were sent (Priority: P1) 🎯 MVP

**Goal**: Close [#227](https://github.com/galax-io/gatling-kafka-plugin/issues/227). A request the
plugin rejected before sending stops contributing a latency sample to the request name the
simulation declared, while staying a KO with its specific error. A protocol that can never work
stops producing one KO per request and refuses to start instead.

**Independent Test**: Run a scenario mixing rejected and completing requests under one declared
name; the percentiles reported for that name equal those of a run containing only the completing
requests, the rejections appear under their own names carrying their real intervals, and
`global.failedRequests` is unchanged. Separately, start a simulation whose protocol has no
`consumeSettings` and confirm it fails during setup with zero request rows.

**Delivers**: FR-001, FR-001a, FR-002 to FR-007, SC-001, SC-001a, SC-002 to SC-006. Contract clauses C1, C2, C3, C4, C6, C7, C13.

### Tests for User Story 1 (MANDATORY — Principle IV) ⚠️

> Write these FIRST. Each must fail against the current code before its implementation task.

- [X] T005 [P] [US1] New spec `src/test/scala/org/galaxio/gatling/kafka/actions/ConsumerSettingsRequiredSpec.scala`: building a request-reply action against `KafkaComponents` whose `trackersPool` is `None` fails at build time with a message naming `consumeSettings`, and zero outcomes are reported (C6). Cover the Java surface by asserting the same builder rejects it, since `javaapi`'s `RequestReplyBuilder` wraps it — see `src/main/java/org/galaxio/gatling/kafka/javaapi/request/builder/RequestReplyBuilder.java`
- [X] T006 [US1] In the same spec, add the negative case that bounds the refusal (C13): a **produce-only** action built from `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestActionBuilder.scala` against the same pool-less `KafkaComponents` builds and executes normally. Absent consumer configuration is the shape `KafkaProtocolBuilder.properties(...)` documents and four simulations plus three published `ProducerSimulation` examples rely on; without this case nothing stops the refusal being widened later
- [X] T007 [P] [US1] In `src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionSpec.scala`, change the existing keyless-request assertion at line 129 so the single reported outcome carries `⟨name⟩ [rejected: no correlation id]`, keeps `KO`, keeps its matcher-and-remedy message verbatim, sends nothing, and still continues the virtual user marked as failed (C1). Update the assertion's comment: the intent it encodes — reported, not swallowed — is unchanged; only which name carries it moves
- [X] T008 [P] [US1] In `src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionSpec.scala`, add the first test of the acquisition-failure reporting path: it reports `⟨name⟩ [rejected: no reply channel]`, `KO`, the underlying failure's message unchanged, and an interval equal to the real wait rather than zero (C2, research.md D3). This path has no test today
- [X] T009 [US1] In `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala`, move `details("Request Reply Keyless Key").failedRequests.count.is(KeylessKeyUsers)` to the derived name, and add an assertion that the declared name `Request Reply Keyless Key` reports **zero** failed requests — the pair is what proves the samples moved rather than being duplicated. Leave `global.failedRequests` and `global.allRequests` pinned at their current values (C3, C4). Depends on T003

### Implementation for User Story 1

- [X] T010 [P] [US1] In `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessages.scala`, add the rejection-kind vocabulary and the derived-name construction — `⟨declared⟩ [rejected: no correlation id]` and `⟨declared⟩ [rejected: no reply channel]` — with a scaladoc recording why the name is the lever (research.md C2, C3: the response-code slot is discarded before a run's data is written) and that the bracketed suffix is documented as reserved rather than provably collision-free (plan.md Complexity Tracking)
- [X] T011 [US1] ~~In `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionBuilder.scala`, resolve `KafkaComponents.trackersPool` inside `build`~~ — **superseded by T012 during implementation.** `KafkaRequestReplyActionBuilder.build` constructs the action, so a precondition in `KafkaRequestReplyAction`'s constructor already fires at build time. One mechanism instead of two, in the layer that owns the invariant, and directly unit-testable without standing up a `ScenarioContext`. The builder is unchanged; it still covers Scala, Java and Kotlin because all three construct the action through it (C6)
- [X] T012 [US1] In `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala`, replace the per-request `case None` arm at lines 181-187 with a single construction-time precondition on the tracker pool. The public constructor signature does not change (Constitution I); the branch is removed rather than left unreachable (Constitution III). Depends on T011
- [X] T013 [US1] In `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala`, report the missing-correlation-id rejection and the acquisition failure under their derived names via `reportFailure`, keeping `logResponse`, `KO`, each message verbatim, and each real measured interval — the acquisition wait is relocated, not flattened (C1, C2, FR-004). Depends on T010, T012
- [X] T014 [US1] Verify the regression clause: run `sbt "Gatling / test"` and confirm `KafkaGatlingTest`, `KafkaConcurrencyLoadTest` and `KafkaJavaapiMethodsGatlingTest` pass with no edits, and that `src/test/scala/org/galaxio/gatling/kafka/integration/KeylessCorrelationSpec.scala` still passes unmodified — it is name-agnostic and proves nothing reaches the broker (C7)

**Checkpoint**: US1 is complete and independently shippable as `fix(stats): … (#227)`. Its Migration
Guide entry (T023) travels in the same PR — Constitution I requires it there.

---

## Phase 4: User Story 2 - A reply that arrived is never reported as one that never came (Priority: P2)

**Goal**: Close [#228](https://github.com/galax-io/gatling-kafka-plugin/issues/228). A service that
answers with a tombstone under value correlation stops being reported as a service that never
answered, and a configuration that does correlate such replies is documented and proved.

**Independent Test**: Point a value-correlated scenario at a responder that answers every request
with a tombstone; no failure in the run reads as a bare reply timeout. Point the same scenario at a
responder that never answers; the failures read as plain reply timeouts. Then run a
header-correlated scenario against the tombstone responder and confirm every reply is matched.

**Delivers**: FR-008 to FR-010, SC-007 to SC-009. Contract clauses C8, C9, C10, C11, C12.

### Tests for User Story 2 (MANDATORY — Principle IV) ⚠️

- [X] T015 [P] [US2] In `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala`, assert that a reply whose correlation id the matcher cannot derive increments the tracker's uncorrelatable-reply count, is dropped as today, and completes or fails no pending request (C8)
- [X] T016 [P] [US2] In `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala`, assert that a request failed by the periodic timeout scan reports `Reply timeout after <N> ms` verbatim when the count is zero, and additionally names how many replies arrived that could not be correlated when it is non-zero (C9). Drive the scan deterministically via the package-visible `TimeoutScan` message rather than waiting on the scheduler
- [X] T017 [P] [US2] In `src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala`, assert the two exclusions: a delivery failure, consumer failure, channel stop, failed check and match-id reuse all report unchanged messages whatever the count is (C10); and a reply whose id **is** derivable but matches no pending request does not change the count and reports nothing (C11)
- [X] T018 [US2] In `src/test/scala/org/galaxio/gatling/kafka/examples/EchoResponder.scala`, echo a named correlation header from the request onto the reply. It currently builds fresh `RecordHeaders` carrying only `x-responded-at` and drops the request's headers at lines 80-88, so a header-correlated reply can never match. Keep the tombstone and probe behaviour exactly as it is
- [X] T019 [US2] In `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala`, add a value-correlated scenario against a tombstone route and assert its failures name the uncorrelatable replies rather than reading as bare timeouts, alongside the existing `scnRRwo`, whose failures must stay plain reply timeouts — the pair is the assertion (C9). Add its contribution to the constants from T003. Depends on T003, T018
- [X] T020 [US2] In `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala`, add a header-correlated scenario against the same tombstone route: every reply is matched, a body check against the absent payload fails cleanly as v1.2.0 established, and the request does **not** time out (C12). Add its contribution to the constants from T003. Depends on T003, T018

### Implementation for User Story 2

- [X] T021 [US2] In `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala`, count replies whose correlation id the matcher cannot derive, on the existing branch at lines 297-300 that already logs and drops them. Actor state on a single-threaded actor, so no synchronisation; an integer increment only — this runs on the thread that gates reply throughput and must neither allocate nor format (C8, C11)
- [X] T022 [US2] In `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala`, extend only the periodic timeout scan's failure text to name the uncorrelatable replies when the count is non-zero, leaving `failPending`'s other callers — delivery failure, consumer failure, `Stop`, match-id reuse — untouched (C9, C10). Depends on T021

**Checkpoint**: US2 is complete and independently shippable as `fix(client): … (#228)`. Its Migration
Guide entry (T024) travels in the same PR.

---

## Phase 5: User Story 3 - An upgrading reader knows which of their numbers moved (Priority: P3)

**Goal**: Every reported figure that can move is named in the Migration Guide, with its direction,
before anyone hits it mid-run.

**Independent Test**: Read the Migration Guide entry for this version against the list of behaviour
changes; every change is named, and the limits this feature does not fix are stated rather than
implied away.

**Delivers**: FR-012, SC-010. Contract clauses C5, C12.

**Note on PR placement**: T023 and T024 do **not** ship as their own PR. Constitution I requires a
Migration Guide entry in the same PR as the change it documents, so T023 rides with US1 and T024
with US2. Only T025, which corrects statements that predate this feature, is a separate
documentation PR under Constitution V.

- [X] T023 [US3] In `README.md`, add the `2.1.0` Migration Guide entry for #227 — rejections move to a derived request name; `details("⟨name⟩").failedRequests` stops counting them while `global.failedRequests` still does; the acquisition wait is relocated, not lost; a missing `consumeSettings` now fails before the run instead of once per request, and a produce-only protocol attached at the `setUp` level alongside one request-reply scenario now stops the whole simulation rather than KOing that one scenario; produce-only simulations themselves are untouched (C13); and the bracketed suffix is reserved. State plainly that `global.responseTime` still blends rejections in, and name `details("⟨name⟩").responseTime` and `failedRequests`/`successfulRequests` as what to read instead (C5). Ships in the US1 PR
- [X] T024 [US3] In `README.md`, add the `2.1.0` Migration Guide entry for #228 beside the existing v1.2.0 absent-payload note: a reply timeout on a channel that received uncorrelatable replies now says so, and header correlation via `matchByMessage` is the required shape for services that may answer with tombstones (C12). Ships in the US2 PR
- [X] T025 [P] [US3] In `README.md`, correct the statements this feature invalidates — the "Immediate rejections … reported with a near-zero response time" paragraph at line 806, and the 1.1.0 acquisition-failure note at line 830 that promises "the same KO, the same error message, and the same response-time span" (the span and message stay true, the name does not). Separate documentation PR

**Checkpoint**: No reader can be surprised by a number that moved.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [X] T026 Run `sbt scalafmtAll scalafmtSbt`, then verify with `sbt scalafmtCheckAll scalafmtSbtCheck compile test` — each issue commit must be green under this on its own (Constitution V)
- [X] T027 Run `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"` — the coverage and topic-contract gate. Any new harness topic must be present in both broker definitions
- [X] T028 Publish under the sentinel version and run all three example consumer projects — `sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2`, then `examples/scala` (sbt), `examples/java` (Maven), `examples/kotlin` (Gradle). All nine examples must pass unchanged; none exercises a rejection or uses `details(...)`, so any failure here is a real regression on the measured path (C7)
- [X] T029 Work the hand-verification steps in [quickstart.md](./quickstart.md) — revert each change locally in turn and confirm the corresponding assertion actually fails — and record the evidence in the PR, as `007-multilang-example-ci-coverage` did for its deliberate-break demonstration
- [ ] T030 **Deferred to PR time — nothing to gate yet.** Verify milestone linkage with `scripts/check-linkage.sh --pr <N>` for each PR: milestone `v2.1.0 Measurement truth`, `Closes #227` / `Closes #228`, and the issue in the same milestone
- [ ] T031 [P] **Deferred — flagged, not done here.** `AGENTS.md` is on this repo's never-commit-without-being-asked list, so it needs an explicit go-ahead. Separate PR: `AGENTS.md`'s Test Model names three Gatling harnesses and omits `KafkaFailureModesGatlingTest`, which the constitution's Full CI gate lists and which this feature extends. Constitution Governance requires `AGENTS.md` be corrected in the PR that surfaces the conflict; it is out of this feature's scope under Constitution V, so it ships on its own

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies
- **Foundational (Phase 2)**: Depends on Setup. T003 blocks T009, T019, T020 — every task that edits the failure-modes harness
- **US1 (Phase 3)** and **US2 (Phase 4)**: Both depend only on Phase 2. They touch disjoint source files and can proceed in parallel
- **US3 (Phase 5)**: T023 depends on US1, T024 on US2, T025 on neither
- **Polish (Phase 6)**: Depends on whichever stories are being shipped

### User Story Dependencies

- **US1 (P1)** — `KafkaRequestReplyAction`, `KafkaRequestReplyActionBuilder`, `KafkaRequestFailureMessages`. No dependency on US2
- **US2 (P2)** — `KafkaMessageTracker`, `EchoResponder`. No dependency on US1
- **US3 (P3)** — documentation of whichever of the two has landed

The only coupling is `KafkaFailureModesGatlingTest.scala`, which all three of T009, T019 and T020
edit. T003 exists to make those edits additive instead of conflicting.

### Within Each User Story

- Tests are written first and must fail before their implementation task (Constitution IV)
- Message and name vocabulary (T010) before the sites that use it (T011, T013)
- The build-time gate (T011) before the branch it removes (T012)
- The counter (T021) before the message that reads it (T022)

### Parallel Opportunities

- T004 runs alongside T003
- T005, T007, T008 are three different assertions in two files — parallel
- T015, T016, T017 target the same file and must be serialised despite the `[P]` intent within the story; run them as one editing pass
- US1's and US2's implementation tasks touch disjoint files and can run concurrently with different developers
- T025 is independent of everything

---

## Parallel Example: User Story 1

```bash
# T005 and T006 share one new file — write them as one pass, the refusal and the
# produce-only case that bounds it. T007 and T008 are a second, independent pass.
Task: "Build-time refusal + its produce-only negative case in src/test/scala/org/galaxio/gatling/kafka/actions/ConsumerSettingsRequiredSpec.scala"
Task: "Derived name for the missing-correlation-id rejection in src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionSpec.scala"
Task: "First test of the acquisition-failure reporting path in the same spec"

# Confirm all four are red before touching src/main:
sbt "testOnly *ConsumerSettingsRequiredSpec *KafkaRequestReplyActionSpec"
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Phase 1 Setup, then Phase 2 Foundational
2. Phase 3 US1, with T023's Migration Guide entry in the same PR
3. **STOP and VALIDATE**: T014 and T028 — the measured path and all nine examples must be untouched
4. Ship as `fix(stats): … (#227)`, milestone `v2.1.0 Measurement truth`, `Closes #227`

US1 alone is a coherent release: the headline metric of every request-reply run becomes true at the
per-request level, and a protocol that can never work stops pretending to measure anything.

### Incremental Delivery

1. Setup + Foundational → the harness can absorb new scenarios
2. US1 → the declared request name reports only requests that were sent → ship
3. US2 → a reply that arrived is never reported as one that never came → ship
4. T025 and T031 → correct the statements that predate this work → ship separately

### Commit and PR Mapping

One issue, one semantic commit, green on its own (Constitution V):

| PR | Commit | Tasks | Closes |
|---|---|---|---|
| Spec | `docs(speckit): add 008-measurement-truth spec/plan/tasks` | — | — |
| 1 | `fix(stats): report pre-send rejections under their own request name (#227)` | T003–T014, T023 | #227 |
| 2 | `fix(client): name uncorrelatable replies in the timeout they cause (#228)` | T015–T022, T024 | #228 |
| 3 | `docs(readme): correct reporting statements invalidated by 2.1.0` | T025 | — |
| 4 | `docs(agents): list the failure-modes harness in the Test Model` | T031 | — |

Every PR carries milestone `v2.1.0 Measurement truth`. A PR without one must not merge.

---

## Notes

- `[P]` = different files, no dependencies. Where three `[P]` tasks share one file (T015–T017), treat them as one editing pass
- Every count in `KafkaFailureModesGatlingTest` is pinned with `is(n)` on purpose, in both directions — a new failure fails the run and so does an expected one that starts passing. Update them; never loosen them
- Verify each test is red before writing its implementation. Two of these paths have no coverage today, so a test that passes immediately is testing the wrong thing
- This feature does not fix `global.responseTime`. Gatling feeds the run-wide digest without reference to request name, and its assertion API has no successful-only scope for response time. Any task that appears to promise otherwise is misread
- The build-time refusal is scoped to `KafkaRequestReplyActionBuilder` and must stay there. Absent consumer configuration is the produce-only shape `KafkaProtocolBuilder.properties(...)` documents, used by `KafkaJavaapiMethodsGatlingTest`, three protocols in `KafkaGatlingTest`, and all three published `ProducerSimulation` examples. T006 is the guard that keeps the refusal from being widened onto them
