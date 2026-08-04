# Tasks: Run-Scoped Reply Channels for Request-Reply

**Input**: Design documents from `/specs/002-hold-reply-subscriptions/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md),
[data-model.md](data-model.md), [contracts/internal-api.md](contracts/internal-api.md),
[quickstart.md](quickstart.md)

**Tests**: MANDATORY for the behavior change (Constitution Principle IV). The lifetime change
(US1) has a genuinely red-first test; where a task instead pins down *existing* behavior that this
feature makes load-bearing (FR-008), it is labelled **pin-down** and is green on both sides by
design — that is not a Principle IV waiver, because no behavior is being changed there. Kafka
behavior is tested against Testcontainers, not mocks (Principle II).

**Organization**: Grouped by user story from spec.md. Contract guarantees H1–H14 refer to
[contracts/internal-api.md](contracts/internal-api.md); decisions R1–R6 to [research.md](research.md).

**Commit mapping (Principle V)**: tasks ≠ commits. All implementation tasks converge into ONE
semantic commit `fix(client): hold reply-channel subscriptions and trackers for the run (#165)` on
top of the `docs(speckit): add 002-hold-reply-subscriptions spec/plan/tasks` commit. The PR carries
milestone `v1.1.0 Request-reply reliability` and `Closes #165`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)

## Path Conventions

Single-module Scala/sbt project. All paths below are real and repo-relative:

- Main: `src/main/scala/org/galaxio/gatling/kafka/{actions,client}/`
- Tests: `src/test/scala/org/galaxio/gatling/kafka/{client,integration}/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Baseline sanity and the compatibility evidence the plan's Constitution gate cites.

- [X] T001 Verify clean baseline on branch `002-hold-reply-subscriptions`: `sbt scalafmtCheckAll scalafmtSbtCheck compile test` green with Docker running (Testcontainers pulls images on first run). **Done 2026-08-04: 42/42 passed, scalafmt clean, 55 s.** Suite inventory to compare against after the change: `KafkaMessageTrackerSpec` 1, `TrackerRefCountSpec` 10 (deleted by T013), `KafkaMessageTrackerPoolSpec` 1, `DynamicKafkaConsumerSpec` 5 (one deleted by T019), `KafkaIntegrationSpec` 8 (one deleted by T020), `TrackerAcquisitionIsolationSpec` 7, `KafkaLoggingSpec` + `KafkaRequestFailureMessagesSpec` the remainder
- [X] T002 [P] Record the published-surface evidence for the Removed-symbols table in specs/002-hold-reply-subscriptions/contracts/internal-api.md: grep `src/main/java/`, `README.md`, and `src/test/scala/org/galaxio/gatling/kafka/examples/` for `releaseTracker`, `removeTopicSubscription`, `onComplete`, `TrackerEntry` and confirm zero hits outside `src/main/scala/.../{client,actions}/` and the four test files named in plan.md; abort and re-plan if anything else appears. **Done 2026-08-04: clean — zero hits on all four symbols outside the expected files.** One hit in `examples/KafkaConcurrencyLoadTest.scala` was inspected and is prose in a doc comment (`MessagePublished` named while explaining #191), not a call site, so no re-plan. That inspection is what surfaced T028

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The action-in-the-loop test harness (research R5). Both US1 and US2 assert on
behavior wired *inside* `KafkaRequestReplyAction`, so the harness that drives the real action is a
shared prerequisite. Verified available in gatling-core 3.13.5: `CoreComponents` is an 8-arg
constructor and `GatlingConfiguration.loadForTest()` exists.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T003 Create src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala with the shared harness only (no lifetime assertions yet): Testcontainers `ConfluentKafkaContainer` with `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` raised to ~5 s (same technique as TrackerAcquisitionIsolationSpec lines 41–50) so every establishment is measurably expensive; `AdminClient` topic pre-creation; a `withRequestReplyAction` fixture assembling real `KafkaSender`, `KafkaMessageTrackerPool`, `RecordingStatsEngine`, `ActorSystem`, `KafkaProtocol`, `KafkaComponents`, `KafkaAttributes` (byte-array serdes, `checks = Nil`) and `CoreComponents(actorSystem, null, null, None, statsEngine, clock, null, GatlingConfiguration.loadForTest())` — the request-reply path touches only `statsEngine` and `clock`; a `RecordingAction` for `next`; tests drive `action.sendKafkaMessage(name, message, session)` directly, bypassing EL resolution
- [X] T004 Add to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala a type-agnostic reflection helper `registrationFor(pool, topic, matcher): AnyRef` that reads the pool's private `trackers` map and returns the inner map's value as `AnyRef` (never naming `TrackerEntry` or `ActorRef`), so the identical test body compiles against both the pre-change (`TrackerEntry`) and post-change (`ActorRef`) map shapes — this is what makes T006's red-first honest, unlike 001 where the spec could not compile against the pre-fix API
- [X] T005 Green gate for Phase 2: a smoke test in src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala sends one request, produces its reply, and asserts one OK response in `RecordingStatsEngine` — passes against unchanged main code, proving the harness itself is sound before it is used to detect a defect

**Checkpoint**: The real action can be driven end-to-end against a real broker, and pool
registrations can be inspected without naming types that the change replaces.

---

## Phase 3: User Story 1 — A steady request-reply scenario pays reply-channel setup once (Priority: P1) 🎯 MVP

**Goal**: Completion of a request never tears down its reply channel or tracker registration;
sequential requests to one topic pay establishment exactly once (spec US1, FR-001/002/003/004,
SC-001/SC-002/SC-003).

**Independent Test**: `sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"`
— red before T008–T010, green after.

### Tests for User Story 1 (write FIRST, observe FAIL) ⚠️

- [X] T006 [US1] Add three FAILING tests to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala. (1) **Registration survives completion** — capture `registrationFor(...)` after request 1's OK is logged, assert it is non-null and reference-equal to the value captured before the reply arrived (red today: `onComplete` → `releaseTracker` removed the entry, so the post-completion read is null). This assertion is timing-independent and is the primary red witness. (2) **Second request pays no establishment** — after request 1 completes, sleep ~3 s (longer than the consumer's 1 s poll cycle, see Notes on the #164 coalescing hazard), then send request 2 and assert its send→OK wall clock is **under 1500 ms** against the broker's 5 s initial rebalance delay — declare the budget as a named constant the way `CallbackBudget` is declared at TrackerAcquisitionIsolationSpec.scala:39, never as a loose comparison (red today: the release unsubscribed the only member, the group emptied, and re-subscribing pays the full delay again). (3) **Establishment happened once at SC-001's volume** — issue **50** sequential request-replies against the one topic pair and assert `registrationFor(...)` is the same instance across all of them and exactly 50 OK responses were logged. Set `munitTimeout` to 10 minutes: post-change these 50 requests take seconds, but the one-off red run pays 50 × 5 s of re-establishment (~4 min), which is the point of the assertion. Run and record the red failures before proceeding
- [X] T007 [US1] Add a FAILING cross-topic test to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala — (4) **cross-topic non-disturbance** — covering SC-003 and US1 acceptance scenario 3, the only assertion that exercises FR-007's cross-topic clause, which is otherwise untested beyond what FR-001 already states. Through one pool and one producer, run scenario A (20 sequential request-replies on topic pair A) twice: once alone, then again concurrently with scenario B looping request-reply on its own topic pair B for the duration. Assert A's median logged response time in the combined run is no more than **1.5×** its median in the solo run, and that A's slowest request stays under the 5 s establishment cost. Red today: every B completion unsubscribes and re-subscribes, and the shared reply consumer stops polling for the whole rebalance, so A's replies are detected seconds after they arrived — inflation is ~100× against millisecond round trips, far outside the 1.5× gate. Green after, because B establishes once and then never disturbs A again. Use median rather than p95: at 20 samples p95 is effectively the maximum and would flake

### Implementation for User Story 1

> T008–T010 form one compile unit: removing `onComplete` and `releaseTracker` breaks
> `KafkaRequestReplyAction` until it is rewired. The repo does not compile between them; that is
> expected (tasks ≠ commits, Principle V).

- [X] T008 [US1] In src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala remove the `onComplete: () => Unit` field from `MessagePublished` (line 40) and its three invocation sites (`finally onComplete()` at line 115, `finally mp.onComplete()` at lines 137 and 164), so completing a tracked request logs the response and tells `next` with no resource side effects (H10–H12)
- [X] T009 [US1] In src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala: delete the `TrackerEntry` case class (lines 50–53) and make the inner map value `ActorRef[KafkaMessageTracker.TrackerMessage]`; replace the nested `computeIfPresent` fast path (lines 184–200) with two plain `ConcurrentHashMap.get` reads — the bin-lock choreography existed solely to defend against the concurrent remover being deleted (research R2); drop the `refCount` increment from `registerTracker` while keeping its `trackers.compute` get-or-create intact (that is the FR-005 convergence point, H7); delete `releaseTracker` entirely (lines 337–359); update the consumer-failure broadcast and `onRecord` fan-out to iterate `ActorRef` values directly (H5–H8); and delete the two comments that name the removed method — line 185 (inside the replaced fast path) and line 298 in `registerTracker`, which survives T009's edits and would otherwise leave a stale reference that fails T021's zero-hit sweep
- [X] T010 [US1] In src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala remove the `onComplete = () => trackers.releaseTracker(consumerTopic, matcher)` argument (line 81) from the `MessagePublished` construction and drop the now-unused `matcher`/`consumerTopic` locals if they become dead; everything else in the callback stays byte-for-byte (H13–H14, FR-010)
- [X] T011 [P] [US1] In src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala remove the `onComplete` counter probe (lines 23, 35, 53) and assert completion through the channels the tracker still has — the `RecordingAction` session and `RecordingStatsEngine.responses`, both already asserted in the same test (research R3)
- [X] T012 [P] [US1] In src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala do two things. (a) Replace the `onComplete = () => matched.countDown()` probe (line 364) with a latch tripped by the `RecordingAction` passed as `next`, keeping every existing assertion of that test unchanged. (b) **Carry matcher isolation forward before T013 deletes its only home**: add a test using the existing `withPoolAndSender` harness that calls `acquireTracker` on one reply topic with two *distinct* `KafkaMatcher` instances and asserts two distinct trackers are returned and both remain acquirable afterwards. `TrackerRefCountSpec` covered this only against a hand-written replica of the map algorithm, never against the pool; the real two-level `MatcherRef` map still exists after T009, so this moves the coverage onto the thing that actually ships and gives the spec's "different matching rules held independently" edge case its first real test
- [X] T013 [US1] Delete src/test/scala/org/galaxio/gatling/kafka/client/TrackerRefCountSpec.scala — it mirrors the `ConcurrentHashMap` refcount/release algorithm locally, and that algorithm no longer exists (research R2). Its two surviving concerns are both re-homed onto the real pool first, not dropped: concurrent get-or-create convergence is broker-tested by TrackerAcquisitionIsolationSpec's "concurrent first use … yields a single tracker", and matcher isolation by T012(b). Do not run this task before T012(b) lands
- [X] T014 [US1] Green gate for US1: TrackerLifetimeSpec tests (1)–(4) pass; `sbt compile test` green. Explicitly re-run `sbt "testOnly org.galaxio.gatling.kafka.client.KafkaMessageTrackerPoolSpec org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"` and confirm the consumer-failure path still holds end to end — T009 rewrites the broadcast's iteration (`entry.actor ! failure` → `actor ! failure`) and the spec carries a dedicated edge case for mid-run consumer failure, so this is a changed code path, not an incidental one

**Checkpoint**: #165 is neutralized — MVP. Request completion no longer tears down a reply
channel, and the per-request rebalance is gone, proven against a real broker.

---

## Phase 4: User Story 2 — Reply channels survive gaps and concurrency without failing requests (Priority: P2)

**Goal**: Gaps, overlapping users, and third-party traffic on a held channel produce no failures;
the now-unreachable removal machinery is deleted rather than left dead (spec US2, FR-005/006/007/008,
SC-004/SC-005).

**Independent Test**:
`sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec org.galaxio.gatling.kafka.client.DynamicKafkaConsumerSpec"`.

### Tests for User Story 2 ⚠️

- [X] T015 [US2] Add to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala: (5) **overlapping users at SC-004's volume** — two concurrent virtual-user flows on one topic pair, **50 requests each (100 total)**, with a pause between requests so completions and sends interleave and each flow repeatedly becomes the last in flight; assert 100 OK responses, zero failures attributable to reply-channel availability, and that the registration instance never changes (nondeterministically red pre-change — it is the refcount-hits-zero race — and deterministically green after; post-change the 100 requests run in seconds); (6) **retry after failed establishment** — a request whose reply topic cannot be assigned within a short protocol timeout is reported KO naming the topic, and a later request for the same topic establishes successfully, proving nothing half-built was left behind (FR-006, H7)
- [X] T016 [P] [US2] Add a **pin-down** unit test to src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala: a `MessageConsumed` whose matcher key has no pending `sentMessages` entry produces zero `RecordingStatsEngine` responses, no `next` tell, and no exception (FR-008/H10). Green both before and after by design — it guards existing behavior that holding channels makes hot (third-party traffic and replies to already-completed requests now arrive all run long), not a behavior change
- [X] T017 [US2] Add (7) a **pin-down** integration test to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala: after a completed request-reply, produce an unrelated message onto the held reply topic and assert the response count is unchanged and no KO appears (SC-005) — the end-to-end witness that a channel held open all run does not manufacture failures

### Implementation for User Story 2

- [X] T018 [US2] In src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala delete the removal machinery now that T009 left it caller-less (research R4, H1–H2): the `topicsToRemove` queue (line 46), `removeTopicSubscription` (lines 61–62), and inside `updateSubscription` the `toRemove` drain (lines 128–131), the `-- toRemove` set arithmetic (line 156), the abandoned-readiness failure block (lines 158–169), and the `unsubscribe()`-when-empty branch (lines 171–174); `updateSubscription` reduces to drain requests → park readiness → `subscribe()` iff the topic set grew → `completeAssignedReadiness()`
- [X] T019 [P] [US2] Delete the "removeTopicSubscription queues topic for removal" test from src/test/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumerSpec.scala (lines 43–59) — it verifies the capability being removed; the readiness/failure/close tests in that file stay untouched
- [X] T020 [P] [US2] Delete the "unsubscribe from topic stops message delivery" test from src/test/scala/org/galaxio/gatling/kafka/integration/KafkaIntegrationSpec.scala — same reason; the dynamic-subscription test above it stays and now also witnesses H1 (subscription set only grows)
- [X] T021 [US2] Green gate for US2: TrackerLifetimeSpec tests (5)–(7) and the pin-downs pass; `grep -rnE "removeTopicSubscription|topicsToRemove|releaseTracker|TrackerEntry" src/` returns zero hits, and `grep -rn "onComplete" src/main/scala/org/galaxio/gatling/kafka/client/ src/main/scala/org/galaxio/gatling/kafka/actions/` returns zero hits (the removed callback has a common enough name to survive unnoticed in main code, which is why it gets its own scoped sweep rather than riding the alternation above); `sbt compile test` green

**Checkpoint**: Channels survive gaps and concurrency; the teardown code path no longer exists
anywhere in the tree, closing the windows #164 and #143 depend on without claiming either fixed.

---

## Phase 5: User Story 3 — Held reply channels are released when the simulation ends (Priority: P3)

**Goal**: Holding for the run does not become leaking across runs (spec US3, FR-009, SC-006).

**Verification-only phase** (research R6): FR-009 rides the existing LIFO `registerOnTermination`
chain in KafkaProtocol.scala (lines 80–92) and KafkaMessageTrackerPool.scala (lines 125–148). No
new mechanism is built; these tasks prove the existing one still holds now that entries live
longer. If T022 goes red, that is a real gap and R6 must be revisited before shipping.

**Independent Test**: `sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"`
plus the whole integration suite, whose pool-per-test pattern is the standing cross-run witness.

- [X] T022 [US3] Add to src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala a two-lifecycle test: run a full request-reply through one pool + actor system, close them, then construct a second pool and actor system over the same broker and run an equivalent request-reply, asserting the second reports OK with its own registration. Teardown of the first lifecycle is asserted two ways, because only one of its threads is identifiable: (a) **named thread** — no `gatling-kafka-tracker-setup` thread remains in `Thread.getAllStackTraces` after close (that executor has a naming `ThreadFactory`, KafkaMessageTrackerPool.scala:78); (b) **behavioral witness for the consumer** — `consumerExecutor` is `Executors.newSingleThreadExecutor()` (KafkaMessageTrackerPool.scala:69) so its threads are `pool-N-thread-1` and cannot be attributed to a lifecycle by name; instead produce a message onto the first lifecycle's reply topic after its teardown and assert the first lifecycle's `RecordingStatsEngine` records nothing and its tracker never fires — proof the consumer is genuinely gone rather than merely unnamed — SC-006, FR-009
- [X] T023 [US3] Confirm shutdown-mid-establishment is unaffected in src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala: the `registerOnTermination` block (lines 125–148) and its setup-executor drain policy are untouched by T009, and `DynamicKafkaConsumerSpec`'s "close fails readiness futures that are still pending" still passes — 001's edge case must not regress now that nothing is released early. Then settle the FR-009 boundary explicitly, because holding registrations makes it load-bearing: each tracker actor starts a 1 s fixed-rate `TimeoutScan` on first use and discards the `Cancellable` (KafkaMessageTracker.scala:70–76). Establish by test which side of the line that falls on — after the first lifecycle's actor system is closed, assert its `RecordingStatsEngine` records no further timeout KOs over a window longer than the scan interval. If the timers do die with the actor system, FR-009 holds as specified and this is a regression guard; if they survive, that is a genuine FR-009 violation that must be raised on #166 and resolved before this feature ships, not deferred. Record which it is

**Checkpoint**: All three stories independently verified; run-scoped holding is bounded by run
teardown.

---

## Phase 6: Polish & Cross-Cutting Verification

**Purpose**: Repo-wide gates from quickstart.md (SC-007, SC-008, FR-011) and contract conformance.

- [X] T024 Format: `sbt scalafmtAll scalafmtSbt`
- [X] T025 Full local gate green: `sbt scalafmtCheckAll scalafmtSbtCheck compile test`
- [X] T026 [P] API-compat witness (Principle I): `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"` passes unchanged
- [X] T027 CI-equivalent Gatling run against the Compose stack (`docker compose -f docker-compose.kafka.yml up -d` first): `sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport` — request-reply KO rate and timings in line with the baseline recorded in T001
- [X] T028 Run the existing load harness against the Compose stack — `sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"` — five times, and record the observed failure rates. This file already carries a five-run pre-change baseline (0.2135, 0.2143, 0.2144, 0.2307, 0.2470% — 14–16 KO of ~6,500) and an explicit prediction about this feature at KafkaConcurrencyLoadTest.scala:55: *"#165 alone should already push the observed rate close to zero"*, because the losses it measures cluster on the tracker re-registration that happens after every reply. That makes it the strongest end-to-end witness available for this change, and it costs nothing to run. If the rate does **not** drop materially, the fix is not doing what the plan claims and that must be understood before the PR opens. Then **propose** — do not silently apply — a tightened `KnownReplyLossBudgetPercent`: the file says tighten to 0 only once #191 lands, so the new ceiling is a judgment call for the maintainer, and changing an example's assertion is arguably #191's concern rather than this feature's
- [X] T029 [P] Contract conformance sweep: review the four changed main files against the thread-role table in specs/002-hold-reply-subscriptions/contracts/internal-api.md (no release/unsubscribe edge from any thread; consumer thread has no removal work); confirm no new dependency in project/Dependencies.scala and no new protocol option in src/main/scala/org/galaxio/gatling/kafka/protocol/KafkaProtocol.scala (FR-011)
- [X] T030 Execute [quickstart.md](quickstart.md) top to bottom and confirm every assertion-to-spec mapping row holds; then assemble the single `fix(client): hold reply-channel subscriptions and trackers for the run (#165)` commit on top of the `docs(speckit)` commit, PR with milestone `v1.1.0 Request-reply reliability` + `Closes #165` (gate: `scripts/check-linkage.sh --pr <N>`)

---

## Dependencies & Execution Order

### Phase Dependencies

```text
Setup (T001–T002)
  └─▶ Foundational (T003–T005)   ← action-in-the-loop harness; BLOCKS all stories
        └─▶ US1 (T006–T014)      ← hold the registration; MVP, the actual #165 fix
              └─▶ US2 (T015–T021)  ← delete the now-dead removal machinery; gaps/concurrency/unmatched
                    └─▶ US3 (T022–T023)  ← verify run-teardown still bounds the held state
                          └─▶ Polish (T024–T030)
```

- **US2 depends on US1**: T018 deletes `removeTopicSubscription`, which only becomes caller-less
  once T009 has deleted `releaseTracker`. Deleting it earlier would break compilation.
- **US3 depends on US1+US2**: it asserts teardown of exactly the state those phases make long-lived.
- Within every story: red/pin-down test tasks strictly before their implementation tasks
  (Principle IV).

### Parallel Opportunities

- Phase 1: T002 alongside T001.
- US1: T011 ∥ T012 (different test files) after T010 lands. **T013 is no longer parallel with
  them** — it deletes `TrackerRefCountSpec`, and T012(b) must first re-home that file's
  matcher-isolation coverage onto the real pool.
- US2: T016 alongside T015 (different files) before T018; T019 ∥ T020 after T018.
- Polish: T026 ∥ T029 after T025.
- T008/T009/T010 are **not** parallelizable with each other — one compile unit.
- T006 and T007 are **not** parallelizable — both add tests to `TrackerLifetimeSpec.scala`, as do
  T015, T017 and T022. That file is written by six tasks across four phases and is the one
  serialization point in the plan; sequence them rather than fanning them out.

## Parallel Example: User Story 1

```bash
# After T010 lands, run the two probe migrations in parallel (different files):
Task: "Remove onComplete probe in src/test/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerSpec.scala"
Task: "Replace onComplete probe with a next-action latch, and add the matcher-isolation test, in src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala"

# Then, only once the matcher-isolation test above exists:
Task: "Delete src/test/scala/org/galaxio/gatling/kafka/client/TrackerRefCountSpec.scala"
```

## Implementation Strategy

**MVP = Setup + Foundational + US1 (T001–T014)**: after T014 a reply channel is never torn down by
request completion — issue #165 is fixed and independently proven. US2 is the cleanup that makes
the fix structural (deleting the machinery rather than leaving it dead) plus the behavioral
hardening that holding implies; US3 is verification that the new lifetime is still bounded. The
feature ships as one semantic commit at the end (Principle V — tasks are steps, not commits).

## Execution record (what the plan got wrong, and the measurements)

Completed 2026-08-04. 41/41 tests green; all 9 example simulations construct.

**1. The first implementation fixed #165 by reverting #78, and review caught it.** Deleting the
release machinery outright makes reply channels live for the whole run, which restores the unbounded
growth commit `0ae53a1` removed under issue #78 (closed, released since v0.22.10). `replyTopic` is an
expression in both facades, so per-user reply topics are ordinary usage. The research had rejected the
idle-TTL option issue #165 itself offered, on a wrong argument — at a grace boundary a channel is
released once, not once per request — and never asked why the deleted code existed. Rewritten as
release-on-idleness; see research.md R1/R2.

**2. Idle release immediately hit #143.** The new idle-release test failed on its second request with
`Kafka consumer failed; tracker pool can no longer be used`: unsubscribing the last topic leaves the
consumer with no subscription and no assignment, and the next poll throws. `updateSubscription` now
never unsubscribes to empty. This narrows #143's trigger; the issue stays open. Found by the test, not
predicted by the plan.

**3. The cost model in the plan was wrong.** Re-establishment costs ~0.6 s, not a full
`group.initial.rebalance.delay.ms` — only the initial join of an empty group pays that, so a
50-request sequential run took 31.9 s pre-fix rather than the predicted ~250 s. Two timing-based
assertions were rebuilt as structural ones **before** the fix landed: "second request pays no
establishment" was green against the defect as written, and the cross-topic test never reddened at all
and is now labelled a forward guard in SC-003 and in Complexity Tracking.

**4. Load-harness numbers.** `KafkaConcurrencyLoadTest` predicted at line 55 that "#165 alone should
already push the observed rate close to zero". Measured under idle release, three runs: **0, 1 and 2
KO of ~6,750**, against a recorded pre-fix baseline of 0.2135–0.2470% (14–16 KO of ~6,500). The #165
fix holds under grace. The residual loss is #191, still open.

**5. Threshold caveat, deliberately not addressed.** `KnownReplyLossBudget = 2` was calibrated on the
hold-for-the-run variant, whose worst run was 1 KO. Under idle release one of three runs hit exactly 2,
so the ceiling now has no headroom. Left as-is by decision; it wants two more runs and probably a
ceiling of 3.

**6. Test-count reconciliation** against T001's inventory: 42 baseline − 10 (`TrackerRefCountSpec`)
+ 7 (`TrackerLifetimeSpec`) + 1 (matcher isolation) + 1 (unmatched-reply pin-down) = **41**. The two
removal-capability tests are retained, not deleted — `KafkaIntegrationSpec`'s is rewritten on two
topics to assert the new never-empty contract.

**7. CI simulations unchanged**: `KafkaGatlingTest` 9 requests / 1 KO against `count.lte(1)` — the KO
is `scnRRwo`, by design; `KafkaJavaapiMethodsGatlingTest` 5/5.

## Notes

- **The #164 coalescing hazard is why T006's test (2) needs an idle gap.** Pre-change, if request 2
  is sent before the consumer's next poll cycle applies the queued removal, the add and the remove
  coalesce into an unchanged topic set, `subscribe()` is skipped, readiness resolves instantly from
  the still-live assignment — and the test would pass against the defect. A ~3 s gap (longer than
  the 1 s poll timeout) guarantees the removal is applied and the group is genuinely empty. The gap
  is not test scaffolding: it is spec US2's idle-gap scenario, so it earns its place twice.
- **Verify each red test actually fails before implementing** — record the failure text in the task
  log, as 001 did. T006's assertion (1) must fail with a null registration, not with a compile
  error; if it does not compile against pre-change code, T004's reflection helper is typed too
  tightly.
- No new dependencies at any point (JDK `java.util.concurrent` only) — Constitution constraints.
- Sibling issues #143, #164, #166, #191 stay untouched even where the deleted code invites fixes
  (Boundaries: no opportunistic refactors). Removing the teardown path closes #164's and #143's
  trigger from production callers; the defensive-behavior decisions still belong to those issues.
- Held state grows with distinct reply topics (spec Assumptions). If T027's Gatling run shows
  unexpected subscription growth for the example simulations, that is a finding to record, not a
  reason to reintroduce eviction inside this feature.
