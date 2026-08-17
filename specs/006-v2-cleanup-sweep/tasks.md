---

description: "Task list for 006-v2-cleanup-sweep"
---

# Tasks: v2.0.0 Cleanup — Validated Removal Sweep

**Input**: Design documents from `/specs/006-v2-cleanup-sweep/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/)

**Tests**: Per Constitution Principle IV, test tasks are mandatory for observable behaviour change and exempt for pure refactors demonstrable by the existing suite passing unchanged. [research.md](./research.md) R5 classifies every story. Three places in this feature are genuinely red-green and are marked **RED** below: the `-Wunused` guard (T033), the re-pointed classpath-isolation assertion (T047) and the strengthened pending-request assertion (T061). Everything else takes the refactor exemption, and qualifies *provably* — what is removed either cannot execute, has never carried a value, or has no caller.

**Organization**: Grouped by user story. Each story is one semantic commit mapping to one GitHub issue (Principle V).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1–US5 from spec.md
- Exact file paths included

## Path Conventions

Single-module Scala/sbt project:

- Scala plugin sources: `src/main/scala/org/galaxio/gatling/kafka/{protocol,actions,client,checks,request}/`
- Java facade: `src/main/java/org/galaxio/gatling/kafka/javaapi/`
- Tests: `src/test/{scala,java,kotlin}/`
- Build truth: `build.sbt`, `project/Dependencies.scala`, `project/plugins.sbt`

## ⚠️ Phase order is execution order, not priority order

Phases follow [research.md](./research.md) R7: **US1 → US2 → US3 → US5 → US4**. This deviates from strict P1→P5 because the stories are *sequentially dependent*, not independent:

- US2's import sweep must not run before US1 deletes the files those imports live in.
- US2's guard goes live early so US3, US5 and US4 physically cannot leave residue behind.
- US5 re-reads the Kotlin examples against the post-US1 surface.
- US4 judges tests against the **final** shape of the library; sweeping first would mean re-judging every test whose subject US1–US3 changed.

**These stories cannot be parallelised across developers.** Parallelism within a phase is marked `[P]`.

## Issue mapping

| Story | Issue | Notes |
|---|---|---|
| US1 (part a) | [#231](https://github.com/galax-io/gatling-kafka-plugin/issues/231) | `Closes #212`, `Closes #210` — both live in earlier milestones (spec.md Assumptions) |
| US1 (part b) | **new issue, T004** | Verdict B1 — dead published surface no existing issue names |
| US2 | [#216](https://github.com/galax-io/gatling-kafka-plugin/issues/216) | Body needs correcting first (T005) — its counts are wrong and it asks for a deletion verdict C1 refuses |
| US3 | [#232](https://github.com/galax-io/gatling-kafka-plugin/issues/232) | |
| US5 | [#181](https://github.com/galax-io/gatling-kafka-plugin/issues/181) | Resolution narrowed by clarification Q3 |
| US4 | [#215](https://github.com/galax-io/gatling-kafka-plugin/issues/215) | |

---

## Phase 1: Setup (issue hygiene and spec landing)

**Purpose**: Make the tracker agree with the audit before any code moves. No source changes.

- [X] T001 Commit the spec artifacts as their own commit (Principle V, spec-first): `specs/006-v2-cleanup-sweep/` and `.specify/feature.json`, message `docs(speckit): add 006-v2-cleanup-sweep spec/plan/tasks`
- [X] T002 Confirm milestone [#15 `v2.0.0 Cleanup`](https://github.com/galax-io/gatling-kafka-plugin/milestone/15) is the target. **Measured**: `scripts/current-milestone.sh` resolves to `16  v1.4.0 Measurement truth` — it sorts open milestones by semver, not by milestone number — so `.claude/hooks/milestone-guard.sh` will block assignment to #15 and `MILESTONE_GUARD_OFF=1` is genuinely required (plan.md Complexity Tracking records why this is a hook limitation, not a principle override)
- [ ] T003 [P] Add a comment to [#215](https://github.com/galax-io/gatling-kafka-plugin/issues/215) noting the overlap with US2: its `KafkaRequestFailureMessagesSpec:19-24` item is removed by #216 together with `buildFailure`, so US4 must not count it twice
- [ ] T004 [P] Open a new issue in milestone #15 for verdict B1 — `KafkaCheckMaterializer.avroBody`, `KafkaMessagePreparer.avroPreparer` and `AvroErrorMapper` are unreachable published surface, citing that both `avroBody` entry points route through `AvroBodyCheckBuilder._avroBody` → `kafkaStatusCheck`
- [ ] T005 [P] Correct the body of [#216](https://github.com/galax-io/gatling-kafka-plugin/issues/216): replace its unused-import estimates with the measured 22 imports across 12 files plus 1 private type (spec.md verdict A10), and strike its `idleSweep.cancel(false)` item, stating that the cancel runs *before* `setupExecutor.shutdown()` so its "already shut down" premise is false (verdict C1)

**Checkpoint**: tracker and audit agree; every removal about to be made has an issue behind it.

---

## Phase 2: Foundational (pre-change baseline capture)

**Purpose**: Snapshot the current state. Once US1 lands, these numbers cannot be recovered without checking out an old commit — and the break-surface record depends on them.

**⚠️ BLOCKING**: no story work may begin until this phase is complete.

- [X] T006 Fetch the baseline artifact `org.galaxio:gatling-kafka-plugin_2.13:1.3.0` from Maven Central into the scratch directory (verified available during research; no credentials needed)
- [X] T007 Extract the baseline public signature set with `javap -public` over every class in the 1.3.0 jar, normalised, into a scratch file — this is the left side of the diff required by [contracts/removed-api.md](./contracts/removed-api.md) R2
- [X] T008 [P] Record the current inherited dependency set by running `sbt makePom` and listing `compile`/`runtime` scopes from `target/scala-2.13/*.pom`. Expected baseline: `scala-library`, `kafka-clients`, `kafka-streams-scala`, `avro`
- [X] T009 [P] Record the current unused-code finding count by running `sbt 'set root / scalacOptions += "-Wunused:imports,privates,locals,patvars"' 'set root / scalacOptions -= "-Xfatal-warnings"' compile Test/compile` and confirming **23 findings**. A different number means the sources moved since the audit and the extra findings need verdicts before deletion (data-model RE-5)
- [X] T010 [P] Record the current `sbt test` outcome as the reference the sweep must not change (SC-005)

**Checkpoint**: baselines captured; US1 can begin.

---

## Phase 3: User Story 1 — Every DSL entry point a simulation can reach actually works (Priority: P1) 🎯 MVP

**Goal**: Remove the published surface that cannot work, plus the dependency chain it holds alive, so every reachable `send(...)` can send and consumers stop inheriting a stream-processing library.

**Independent Test**: `ExampleSmokeValidation` constructs every README and example simulation; `sbt makePom` shows three inherited dependencies with no `kafka-streams-scala`; `checkPublishedPom` passes with no deprecation allowance.

**Two commits**: T011–T028 close #231; T029–T032 close the B1 issue from T004.

### Commit 1 — the deprecated surface and its chain (#231)

**Ordering constraint (research R4)**: the implicits must go **before** the dependency, in the same commit. Their declared types appear in signatures implicit search reads for every simulation.

- [X] T011 [US1] Remove `sessionWindowedSerde`, `consumedFromSerde`, their `@deprecated` annotations, the `WindowedSerdes` and `Consumed` imports, and the ~20-line comment block explaining why they could not go, in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala`
- [X] T012 [US1] Remove `kafka-streams-scala` from `Dependencies.kafka` in `project/Dependencies.scala`, leaving `kafka-clients` alone
- [X] T013 [US1] Remove the `kafka-streams` and `kafka-streams-scala` entries from `kafkaOverrides` in `project/Dependencies.scala`. **Keep `kafka-clients`** — it is pinned against Confluent's vendor rebuild, a separate concern (research R4)
- [X] T014 [US1] Remove the `"org.apache.kafka:kafka-streams-scala" -> "deprecated: ..."` entry from `inheritedDependencyJustification` in `build.sbt`
- [X] T015 [US1] Remove rule DR-4 from `checkPublishedPom` in `build.sbt`: the `heldByDeprecation` computation, its failure branch, and the `deprecated:` prefix convention in the map's doc comment. Leave C1, C2, C3 and C5 intact
- [X] T016 [US1] Remove the topic-less `send(...)` overloads from `case class KafkaRequestBuilderBase` in `src/main/scala/org/galaxio/gatling/kafka/request/builder/KafkaRequestBuilderBase.scala`, including the `if (key == null)` sentinel branch. Keep `topic(...)`, `requestReply`, and the nested `OnlyPublishStep` / `ReqRepBase`
- [X] T017 [US1] Remove the topic-less `send(...)` matrix and both `sendWithClass(...)` overloads from `src/main/java/org/galaxio/gatling/kafka/javaapi/request/builder/KafkaRequestBuilderBase.java`, keeping only the constructor, `topic(String)` and `requestReply()`. The headers-taking `sendWithClass` is the one that throws `IllegalArgumentException` from `Serdes.serdeFrom(Object.class)` before a scenario can be built
- [X] T018 [US1] Change `producerTopic` from `Option[Expression[String]]` to `Expression[String]` in `src/main/scala/org/galaxio/gatling/kafka/request/builder/KafkaAttributes.scala`, and update the **three** surviving construction sites in `KafkaRequestBuilderBase.scala` — `:18` and `:37` in `OnlyPublishStep`, `:116` in `RROutTopicStep` (the other three set `None` and are deleted by T016). Authorised by verdict **B2**; record it as a cascade of A3 under [contracts/removed-api.md](./contracts/removed-api.md) R3
- [X] T019 [US1] Remove `missingProducerTopicError` and collapse `resolveProducerTopic` in `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaAction.scala` — with T018 the absent-topic state cannot occur, and a diagnostic for an impossible state is the residue this feature exists to clear (contract S1)
- [X] T020 [P] [US1] Remove the `responseCode` field and its `@param` scaladoc from `src/main/scala/org/galaxio/gatling/kafka/request/KafkaProtocolMessage.scala`. It is the last parameter, so positional construction of the first five arguments stays source-compatible (data-model KM-1)
- [X] T021 [US1] Replace the two `message.responseCode` forwards on the reply-received paths with an explicit `None` in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala`. **Do not touch** `failPending` or `executeNext`'s `responseCode` parameter — that slot carries real failure types (data-model KM-3, contract S2)
- [X] T022 [US1] Remove `responseCode` from `describeMessage` in `src/main/scala/org/galaxio/gatling/kafka/package.scala`, keeping topics, key, value and header count
- [X] T023 [US1] Update the expected trace-line string in `src/test/scala/org/galaxio/gatling/kafka/KafkaLoggingSpec.scala` to the new exact text. **Do not relax it to a substring match** (data-model KM-4)
- [X] T024 [P] [US1] Remove the `ResponseCode` constant from `src/main/java/org/galaxio/gatling/kafka/javaapi/checks/KafkaCheckType.java`, leaving `Simple`
- [X] T025 [US1] Remove the `case KafkaCheckType.ResponseCode` branch from `toScalaCheck` in `src/main/java/org/galaxio/gatling/kafka/javaapi/checks/KafkaChecks.scala` — it is byte-identical to the `Simple` branch beside it, so the collapse changes no behaviour for any constructible check
- [X] T026 [P] [US1] Remove `timeout(FiniteDuration)` and `withDefaultTimeout` from `KPProducerSettingsStep` in `src/main/scala/org/galaxio/gatling/kafka/protocol/KafkaProtocolBuilder.scala`. **Keep both on `KPConsumeSettingsStep`** — the example suite uses that one (contract S6)
- [X] T027 [US1] Add an `### Upgrading to 2.0.0` section to the Migration Guide in `README.md` covering: the topic-less `send(...)` removal and the topic-first replacement; the `kafka-streams-scala` removal and how to declare it directly; the `responseCode` field removal with an explicit statement that reported failure types are unaffected; `KafkaCheckType.ResponseCode` → `Simple`; and the producer-scoped timeout removal (FR-006, Constitution I requires this in the same PR as the break)
- [X] T028 [US1] Verify commit 1: `sbt scalafmtAll scalafmtSbt` then `sbt scalafmtCheckAll scalafmtSbtCheck compile test`, `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"`, `sbt makePom` showing three inherited dependencies, and `sbt checkPublishedPom` passing. Commit as `feat(request)!: remove the deprecated surface and everything it holds alive (#231)` with `Closes #231`, `Closes #212`, `Closes #210`

### Commit 2 — the unreachable Avro check path (B1)

- [X] T029 [US1] Remove `def avroBody[T <: GenericRecord: Serde](configuration, topic)` from `src/main/scala/org/galaxio/gatling/kafka/checks/KafkaCheckMaterializer.scala`
- [X] T030 [US1] Remove `avroPreparer[T]` and the `AvroErrorMapper` string from `src/main/scala/org/galaxio/gatling/kafka/checks/KafkaMessagePreparer.scala`. **Keep `withPayload` and `safely`** — they serve the string, JSON, JMESPath and XML preparers (contract S3)
- [X] T031 [US1] Update the comment in `src/main/scala/org/galaxio/gatling/kafka/checks/AvroBodyCheckBuilder.scala` that refers to `KafkaMessagePreparer.avroPreparer`, so it no longer names a deleted symbol while still explaining why the absent-payload guard lives in the extractor
- [X] T032 [US1] Verify and commit commit 2: full gate plus `sbt "testOnly org.galaxio.gatling.kafka.checks.*"`, message `refactor(checks)!: remove the unreachable Avro check preparer path (#<T004 issue>)`

**Checkpoint**: every reachable `send` can send; inherited set is three; both CI Gatling simulations pass.

---

## Phase 4: User Story 2 — Dead code gone, and it cannot silently return (Priority: P2)

**Goal**: Remove the unreachable internals, then make the compiler enforce that the import- and privates-level residue stays gone.

**Independent Test**: build compiles clean with the guard on and `-Xfatal-warnings` in force; a deliberately re-introduced unused import fails the build; no suppression exists anywhere.

- [ ] T033 [US2] **RED** — add `-Wunused:imports,privates,locals,patvars` to `scalacOptions` in `build.sbt` and run `sbt clean compile Test/compile`. Confirm the build **fails** and enumerates the findings. This red state is the test for T034–T041 (research R5)
- [ ] T034 [P] [US2] Remove `buildFailure` from `src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessages.scala`, and the three cases that exercise only it from `src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestFailureMessagesSpec.scala` — "build failures keep request construction wording", "build and send failures remain distinguishable", "buildFailure with null error string falls back to unknown error"
- [ ] T035 [P] [US2] Remove the package-private `bytes(String)` method from `src/main/java/org/galaxio/gatling/kafka/javaapi/request/expressions/ExpressionBuilder.java`
- [ ] T036 [P] [US2] Remove the unused `private type KafkaCheckMaterializer` alias from `src/main/scala/org/galaxio/gatling/kafka/checks/AvroBodyCheckBuilder.scala`
- [ ] T037 [US2] Remove `completionCause` from `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala` and pass `error` straight to `onFailure` in the readiness continuation. The readiness is a plain `CompletableFuture` completed with raw exceptions and `whenCompleteAsync` is registered on it directly, so no `CompletionException` can arrive (verdict A9)
- [ ] T038 [US2] **Do not remove `idleSweep.cancel(false)`** from the termination hook in `src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala`. Verdict C1 refuses this deletion — the cancel runs before `setupExecutor.shutdown()`, with a consumer-future wait and a continuation drain in between. Add a one-line comment recording that the ordering is deliberate, so the next audit does not re-derive it
- [ ] T039 [P] [US2] Remove the 7 unused imports in `src/main/`: `actions/KafkaRequestAction.scala`, `checks/AvroBodyCheckBuilder.scala`, `checks/KafkaCheckSupport.scala`, `client/DynamicKafkaConsumer.scala` (three), `protocol/KafkaProtocol.scala`
- [ ] T040 [P] [US2] Remove the 15 unused imports in `src/test/`: `examples/KafkaGatlingTest.scala` (nine), `client/DynamicKafkaConsumerSpec.scala` (two), and one each in `examples/BasicSimulation.scala`, `examples/KafkaJavaapiMethodsGatlingTest.scala`, `examples/ReadmeExamplesCompileOnly.scala`, `integration/TrackerAcquisitionIsolationSpec.scala`
- [ ] T041 [P] [US2] Correct the two stale narrations: the comment describing removed rethrow behaviour in `src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala`, and the scaladoc naming the deleted `KafkaProtocolBuilderNew` in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaProtocolMessage.scala`
- [ ] T042 [P] [US2] Remove the empty `avroSchemas` val, the commented-out `RegistrySubject` import, the `schemaRegistrySubjects ++= avroSchemas` line and the commented-out `schemaRegistryUrl` line from `build.sbt`
- [ ] T043 [US2] **GREEN** — `sbt clean compile Test/compile` succeeds with zero warnings, and every finding from T033 is accounted for by T034–T042. A finding without a ledger entry needs a verdict in spec.md before deletion (data-model RE-5, UG-2)
- [ ] T044 [US2] Prove the guard bites: add an unused import to any source file, confirm `sbt compile` fails and names it, then revert
- [ ] T045 [US2] Confirm no suppressions bought the green — `grep -rn "nowarn" src/ build.sbt project/` returns nothing (SC-003, UG-1)
- [ ] T046 [US2] Verify and commit: full gate, message `refactor: remove dead code and wire -Wunused so it cannot regress (#216)` with `Closes #216`

**Checkpoint**: guard is live for every story that follows.

---

## Phase 5: User Story 3 — The two constructs the binary freeze forced (Priority: P3)

**Goal**: Express directly what the 1.x binary freeze forced into wrappers, while keeping the classpath-isolation guarantee enforced.

**Independent Test**: `PlainClasspathIsolationSpec` passes with its positive control intact and its re-pointed case asserting the new failure boundary; the Avro Gatling scenarios run end to end.

- [ ] T047 [US3] **RED** — rewrite the `LazyGenericAvroSerde` case in `src/test/scala/org/galaxio/gatling/kafka/classpath/PlainClasspathIsolationSpec.scala` to assert the new boundary: initialising `Predef$` under the denying loader succeeds, while *summoning* `avroSerde` fails with `NoClassDefFoundError`. Against the current code the summon returns a `LazyGenericAvroSerde` without failing, so this test must fail first. Keep all four entry-point cases and the positive control untouched (FR-015, contract S4)
- [ ] T048 [US3] Change `implicit val avroSerde: Serde[GenericRecord] = new LazyGenericAvroSerde` to `implicit def avroSerde: Serde[GenericRecord] = ConfluentSerdes.newAvroSerde()` in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala`, and replace the "MUST stay a strict val" comment block with one explaining that a `def` body runs only on summon
- [ ] T049 [US3] Change `val avroSerde` to `def avroSerde: Serde[GenericRecord] = ConfluentSerdes.newAvroSerde()` in `src/main/java/org/galaxio/gatling/kafka/javaapi/checks/KafkaChecks.scala`. The `avroSerde()` accessor Java calls is unchanged, and the `KafkaChecks$` initialiser then has nothing Avro-related in it at all
- [ ] T050 [US3] Delete `final class LazyGenericAvroSerde` from `src/main/scala/org/galaxio/gatling/kafka/request/ConfluentSerdes.scala` and update the object's scaladoc, which currently explains the wrapper as the mechanism satisfying Contract E1
- [ ] T051 [US3] Fold `trait RequestBuilder[+K, +V]` into `KafkaRequestBuilder` in `src/main/scala/org/galaxio/gatling/kafka/request/builder/KafkaRequestBuilder.scala`, then delete `src/main/scala/org/galaxio/gatling/kafka/request/builder/RequestBuilder.scala`
- [ ] T052 [US3] Re-point `kafkaRequestBuilder2ActionBuilder` at `KafkaRequestBuilder` in `src/main/scala/org/galaxio/gatling/kafka/KafkaDsl.scala`, and update the `send(...)` return types in `src/main/scala/org/galaxio/gatling/kafka/request/builder/KafkaRequestBuilderBase.scala`
- [ ] T053 [US3] Re-point the Java wrapper `src/main/java/org/galaxio/gatling/kafka/javaapi/request/builder/RequestBuilder.java` at the concrete Scala `KafkaRequestBuilder`. This class stays — it is a different type in a different package and implements Gatling's `ActionBuilder`
- [ ] T054 [US3] Extend the `### Upgrading to 2.0.0` section in `README.md` with the two US3 breaks: `LazyGenericAvroSerde` is gone (no source change needed — `Predef` still supplies the serde), and `send(...)` now returns `KafkaRequestBuilder`, so code declaring `RequestBuilder[K, V]` as a type must change
- [ ] T055 [US3] Verify and commit: full gate, `sbt "testOnly org.galaxio.gatling.kafka.classpath.PlainClasspathIsolationSpec"` green with T047 now passing, and `sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"` covering the Avro scenarios. Message `refactor(request)!: drop the constructs the binary freeze forced (#232)` with `Closes #232`

**Checkpoint**: last story that changes a published signature — the break-surface record (T073–T075) can now be produced.

---

## Phase 6: User Story 5 — The Kotlin examples are correct (Priority: P5)

**Goal**: A Kotlin user can copy any example and have it compile. Layout unchanged, no toolchain added (clarification Q3).

**Independent Test**: all four files compile in a scratch Kotlin project depending on the built plugin.

- [ ] T056 [US5] Fix `src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples/ProducerSimulation.kt`: balance the `.exec(` chain so paren depth returns to zero before `init {`, and resolve the six undeclared references — `MyAvroClass`, `Serializer`, `Deserializer`, `KafkaAvroSerializer`, `KafkaAvroDeserializer`, `CachedSchemaRegistryClient` — by adding imports and defining or substituting the Avro type, following the pattern in the sibling `AvroClassWithRequestReplySimulation.kt`
- [ ] T057 [P] [US5] Re-read all four Kotlin examples against the post-US1 surface. Checked during research: they use only `topic(...).send(...)` and `requestReply()...send(...)`, neither removed — so this task confirms rather than restructures
- [ ] T058 [US5] Verify by compiling as a user would: `sbt publishLocal`, then a throwaway Kotlin/Gradle project outside this repository depending on that artifact plus `gatling-core-java` and `gatling-charts-highcharts`, with all four files copied in. Record the result in the PR body; check nothing about the scratch project in
- [ ] T059 [US5] Confirm the constraints held: the four `.kt` files are still in `src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples/`, and `grep -in kotlin build.sbt project/plugins.sbt project/Dependencies.scala` returns nothing (FR-023, SC-007)
- [ ] T060 [US5] Commit as `fix(examples): repair the Kotlin producer simulation (#181)` with `Closes #181`

**Checkpoint**: examples compile against the final API.

---

## Phase 7: User Story 4 — Only tests that can fail (Priority: P4)

**Goal**: Remove ~590 lines of tests that cannot fail or duplicate others, strengthen the one that should have been strong, and leave the race-pinning specs alone.

**Independent Test**: `sbt test` gives the same pass/fail outcome as the T010 baseline, with the removed cases gone and every removal naming a survivor in the commit body.

**Governing rules**: data-model TR-1 (no survivor, no removal), TR-2 (strengthen, never remove), TR-3 (races untouched), TR-4 (no mock replaces a broker test), TR-5 (examples are documentation).

- [ ] T061 [US4] **RED** — strengthen the "stays pending until the assignment actually happens" assertion in `src/test/scala/org/galaxio/gatling/kafka/integration/KafkaIntegrationSpec.scala` to wait several poll cycles against a topic that cannot be assigned, instead of asserting nothing beyond 1 ms. Demonstrate it failing against a deliberately reverted guard, then restore the guard and keep the assertion (FR-019, TR-2)
- [ ] T062 [US4] Remove the cross-topic guard from `src/test/scala/org/galaxio/gatling/kafka/integration/TrackerLifetimeSpec.scala` — it cannot fail against the defect it cites — together with its orphaned helpers (`median()`, `ReuseBudget`, and the docstring only it referenced). Name the survivor in the commit body
- [ ] T063 [US4] Resolve the vacuous post-close silence block in `TrackerLifetimeSpec`: either delete it, or make the guard real by closing while a deliberately unanswered request is in flight. If deleted, name the survivor
- [ ] T064 [US4] Remove the harness smoke test from `TrackerLifetimeSpec`, naming test (1) plus `ReplyRegistrationRaceSpec` as the survivors that cover the same ground
- [ ] T065 [P] [US4] Remove the shadowed test from `src/test/scala/org/galaxio/gatling/kafka/integration/ConsumerStartupSpec.scala`, naming its public-path sibling in the same file as the survivor
- [ ] T066 [US4] Fold the "confirmations keep flowing" test in `src/test/scala/org/galaxio/gatling/kafka/integration/TrackerAcquisitionIsolationSpec.scala` into test 1 — carry over its five sends and the latency assertion so no mutant it killed survives
- [ ] T067 [P] [US4] Remove the state-mirror and doubly-shadowed tests from `src/test/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumerSpec.scala`, and merge the #143 pair into one "an idle consumer neither fails nor refuses a late topic". **Keep** the four fail-fast tests — each kills a mutant no integration test covers
- [ ] T068 [US4] Condense the permutation runs in `TrackerLifetimeSpec`: fold the 50-request sequential test into test (2), and reduce test (7) from 20 channels to the 3 that kill every named mutant. **Do not remove tests (2), (6) or (7) themselves** — they pin live races and the redesign that retires them is not part of this feature (FR-022, TR-3). Because test (7) is a protected test being weakened rather than merged, demonstrate the 3-channel form still fails against a deliberately reintroduced scan leak before keeping it; if it does not, keep 20
- [ ] T069 [P] [US4] Remove the byte-identical duplicate protocol pair (`kafkaConf` / `kafkaConfwoKey`) in `src/test/scala/org/galaxio/gatling/kafka/examples/KafkaGatlingTest.scala`, pointing both injections at the single surviving definition
- [ ] T070 [P] [US4] In `src/test/scala/org/galaxio/gatling/kafka/examples/BasicSimulation.scala`, reduce the five identical scenario injections to the distinct ones the example actually demonstrates, and remove the commented-out `kafkaConf` block plus `getHeader`, which is reachable only from a commented-out `.matchByMessage`
- [ ] T071 [US4] Confirm every removal in T062–T070 names its survivor in the commit body, and that no removal was compensated by adding a mock (TR-1, TR-4)
- [ ] T072 [US4] Verify and commit: `sbt test` matching the T010 baseline outcome, full gate, and `sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"` showing tests (2), (6) and (7) still present. Message `test: remove tests that cannot fail or duplicate others (#215)` with `Closes #215`

**Checkpoint**: all five stories complete.

---

## Phase 8: Release gate

**Purpose**: The irreversible step. Nothing here is optional — a Sonatype release cannot be withdrawn.

- [ ] T073 Extract the post-change public signature set with `javap -public` over the built jar, and diff it against the T007 baseline ([contracts/removed-api.md](./contracts/removed-api.md) R2). Do not hand-transcribe
- [ ] T074 Match every diff entry to a verdict — `A1`–`A12`, `B1`, or a US3 freeze artifact. Expect cascades from T018 (`KafkaAttributes.producerTopic`), T020 (`KafkaProtocolMessage` `apply`/`unapply`/`copy` arity) and T051 (`send(...)` return type). **An entry with no verdict blocks the release**: record it as a cascade naming its parent, revert it as a mistake, or add a verdict to `spec.md` with evidence and re-run (R3, RE-5)
- [ ] T075 Complete `specs/006-v2-cleanup-sweep/contracts/removed-api.md` with the sections required by R5 — baseline, removed, changed, cascades, dependencies before/after, reviewer note — replacing its status block, and check it in
- [ ] T076 Confirm every `published` entry in the record also appears in the `README.md` migration guide, and vice versa (R7, RE-2)
- [ ] T077 Run the full CI equivalent with the Compose stack up: `sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest" test coverageOff coverageReport`
- [ ] T078 Run `scripts/check-linkage.sh` against milestone #15: every issue closed, every PR merged, each PR carrying a milestone and a `Closes #NNN`
- [ ] T079 Confirm `scripts/check-linkage.sh --for-tag v2.0.0` resolves milestone #15. **Measured during T002**: its title `v2.0.0 Cleanup` already starts with the exact version, so the gate resolves and no milestone rename is needed — this task verifies rather than creates
- [ ] T080 Walk [quickstart.md](./quickstart.md) end to end as the final acceptance pass

---

## Dependencies & Execution Order

### Phase dependencies

- **Phase 1 (Setup)**: no dependencies
- **Phase 2 (Foundational)**: depends on Phase 1 — **BLOCKS everything**; the baselines cannot be recovered afterwards
- **Phase 3 (US1)** → **Phase 4 (US2)** → **Phase 5 (US3)** → **Phase 6 (US5)** → **Phase 7 (US4)**: strictly sequential, per research R7
- **Phase 8 (Release gate)**: depends on Phase 5 for the record's content and on Phase 7 for the suite

### Story dependencies — these are NOT independent

| Story | Blocked by | Why |
|---|---|---|
| US1 | Phase 2 | Baseline must exist before the surface changes |
| US2 | US1 | Sweeping imports out of files US1 deletes is wasted work |
| US3 | US2 | Guard live first, so T047–T053 cannot leave residue |
| US5 | US1 | Examples are re-read against the post-removal surface |
| US4 | US1, US2, US3 | Tests are judged against the final shape of the library |

### Within US1

T011 → T012 → T013 (implicits before the dependency, research R4). T016 → T017 → T018 → T019 (the builder chain before the attribute change before the action). T020 → T021 → T022 → T023 (field, then forwards, then log line, then its assertion). T024 → T025.

### Parallel opportunities

Real, and confined within a phase:

- **Phase 1**: T003, T004, T005 — three independent tracker edits
- **Phase 2**: T008, T009, T010 — three independent measurements
- **Phase 3**: T020, T024, T026 touch different files from the builder chain
- **Phase 4**: T034, T035, T036, T039, T040, T041, T042 — all different files, all mechanical
- **Phase 7**: T065, T067, T069, T070 — four different test files

---

## Parallel Example: Phase 4 (US2)

```bash
# After T033 goes red, these seven are independent files:
Task: "Remove buildFailure + 3 spec cases in actions/KafkaRequestFailureMessages.scala and its spec"
Task: "Remove ExpressionBuilder.bytes(String) in javaapi/request/expressions/ExpressionBuilder.java"
Task: "Remove the private type alias in checks/AvroBodyCheckBuilder.scala"
Task: "Remove 7 unused imports in src/main/"
Task: "Remove 15 unused imports in src/test/"
Task: "Correct the two stale narrations"
Task: "Remove avroSchemas and commented scaffolding in build.sbt"
```

---

## Implementation Strategy

### MVP (US1 only)

Phases 1 → 2 → 3. That alone delivers the reason 2.0.0 exists: no unusable entry point survives, and consumers stop inheriting `kafka-streams-scala`. **Stop and validate** with `ExampleSmokeValidation`, `checkPublishedPom`, and both CI Gatling simulations before going further.

### Incremental delivery

Each phase is one or two semantic commits, green on its own, and each is a coherent thing to review:

1. Phase 3 → the break, with its migration guide (2 commits)
2. Phase 4 → residue gone, guard live (1 commit)
3. Phase 5 → freeze artifacts gone (1 commit)
4. Phase 6 → Kotlin example fixed (1 commit)
5. Phase 7 → test sweep (1 commit)
6. Phase 8 → record, review, tag

### Not parallelisable across developers

Unlike a feature-addition plan, these stories share the same files and each reads the surface the previous one leaves. Splitting them across people produces merge conflicts in `KafkaRequestBuilderBase`, `KafkaSerdesImplicits` and `build.sbt`, and defeats the point of running the test sweep last.

---

## Notes

- `[P]` = different files, no dependencies on incomplete tasks
- One issue = one semantic commit, green on its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test` (Principle V)
- Breaking commits carry `!` so git-cliff and the derived version are correct (FR-025)
- Migration-guide prose ships **with** the story that causes the break, not as a separate docs PR — Constitution I requires the README entry in the same PR
- **Do not delete opportunistically.** Anything found dead during implementation that has no verdict gets a verdict in `spec.md` with evidence first (data-model RE-5). Three claims in the source issues did not survive that bar
- Verdict **C1 keeps code the issue asks to delete** — T038 exists to make sure a well-meaning implementer does not "finish the job"
