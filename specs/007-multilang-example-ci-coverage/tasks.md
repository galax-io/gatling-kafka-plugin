---

description: "Task list for 007-multilang-example-ci-coverage"
---

# Tasks: Multi-Language Example Coverage in CI

**Input**: Design documents from `/specs/007-multilang-example-ci-coverage/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/example-coverage.md](./contracts/example-coverage.md),
[quickstart.md](./quickstart.md)

**Tests**: Per Constitution Principle IV, every behavior change lands with a check that fails before
it and passes after. This feature's "behavior" is CI coverage, so the red-first step is literal: the
examples were run *before* any of them was corrected, so the broken ones were observed failing rather
than asserted to have been broken. Eight of the nine JVM examples failed that first pass. Per
Principle II, every covered example runs against a real broker — the CI stack or
`docker-compose.kafka.yml`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1 = the published examples run; US2 = the gate asserts what it claims; US3 = Kotlin is covered (compile-only as originally scoped; it now runs too — see Phase 7)

## Path Conventions

Current, after Phase 8. The tasks below were written against earlier layouts and name the old paths;
they are left as the record of what was done at the time.

- Scala examples: `examples/scala/src/test/scala/org/galaxio/examples/scalaapi/` — `sbt "Gatling / test"`
- Java examples: `examples/java/src/test/java/org/galaxio/examples/javaapi/` — `mvn verify`
- Kotlin examples: `examples/kotlin/src/gatling/kotlin/org/galaxio/examples/kotlinapi/` — `./gradlew gatlingRun --all`
- Plugin build truth: `build.sbt`, `project/Dependencies.scala`, `project/plugins.sbt`
- Example build truth: `examples/scala/build.sbt`, `examples/java/pom.xml`, `examples/kotlin/build.gradle.kts`
- CI truth: `.github/workflows/ci.yml`

---

## Phase 1: Setup

**Purpose**: Establish the baseline and verify the two assumptions the whole plan rests on, before
any code is written against them.

- [X] T001 Commit the spec artifacts on their own as `docs(speckit): add 007-multilang-example-ci-coverage spec/plan/tasks` (Principle V — spec-first, never folded into implementation)
- [X] T002 Record the pre-feature baseline: run `sbt scalafmtCheckAll scalafmtSbtCheck compile test` and note the result, so any later red is attributable to this feature
- [X] T003 **Verify R1 empirically**: confirm `sbt "Gatling / testOnly org.galaxio.gatling.kafka.javaapi.examples.BasicSimulation"` selects nothing and exits 0. This is the fingerprint blocker from [research.md](./research.md) R1; if it unexpectedly runs, stop and revise the plan before proceeding
- [X] T004 **Verify R7 empirically**: in a scratch REPL or throwaway main, call `newInstance()` on all nine JVM example classes with the broker stopped, and confirm none opens a connection during construction. Any example that does falls to FR-004 as `CompileOnly` and must be recorded before T020

**Checkpoint**: The two load-bearing assumptions are confirmed or the plan is revised.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The example inventory is shared by both P1 stories — the gate reads it to know what to
construct (US2), the runner reads it to know what to run and to check topic disjointness (US1).
Neither story can be built correctly on a hand-maintained list.

**⚠️ CRITICAL**: No user story work begins until T007 is complete.

- [X] T005 Create `src/test/scala/org/galaxio/gatling/kafka/examples/ExampleInventory.scala` — scan the three example source directories and derive `(language, fqcn, sourcePath)` per DR-1 in [data-model.md](./data-model.md)
- [X] T006 In `ExampleInventory.scala`, add the closed exclusion list of test harnesses and utilities (`KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`, `KafkaConcurrencyLoadTest`, `GatlingRunner`, `ReadmeExamplesCompileOnly`, `ExampleSmokeValidation`, `ExampleSimulationRunner`, `ExampleInventory`) with a comment stating why each is not an example
- [X] T007 In `ExampleInventory.scala`, add the coverage-level assignment (`Executed` / `Compiled` / `CompileOnly(reason)`) and make an example with no assigned level a hard error (DR-5, contract C3)

**Checkpoint**: The inventory derives 13 examples from disk — 5 Scala, 4 Java, 4 Kotlin — and fails when one is unaccounted for.

---

## Phase 3: User Story 1 — Published JVM examples run against a broker (Priority: P1) 🎯 MVP

**Goal**: All nine published JVM example simulations execute against the CI broker on every run, with
assertions that fail when an example does nothing.

**Independent Test**: Break one covered example at run time only (it still compiles) and confirm the
example run goes red; revert and confirm green. Delivers value with US2 and US3 untouched.

### Red first — build the runner, then watch the broken examples fail

> Order matters here. The runner is pointed at all nine examples *before* any of them is corrected,
> so the six defects in [research.md](./research.md) R3 are observed rather than assumed.

- [X] T008 [US1] Add the `ExampleRun` sbt configuration to `build.sbt`, extending `Test` with `fork := true` and the `--add-opens=java.base/java.util` / `--add-opens=java.base/java.lang` options the `Gatling` config already sets (R5) — do NOT set `Test / fork := true`
- [X] T009 [US1] Create `src/test/scala/org/galaxio/gatling/kafka/examples/ExampleSimulationRunner.scala` driving `io.gatling.app.Gatling.fromArgs` once per example, aggregating status codes, with an optional FQCN argument to run one example (R1)
- [X] T010 [US1] In `ExampleSimulationRunner.scala`, add the topic-disjointness pre-check (contract C6): fail before running anything if two covered examples share a topic
- [X] T011 [US1] Add the `exampleRun` task to `build.sbt` wiring the runner into the `ExampleRun` configuration
- [X] T012 [US1] Add the nine new topics from [data-model.md](./data-model.md) to `KAFKA_CREATE_TOPICS` in `.github/workflows/ci.yml`, keeping the existing entries and the comment that this list is maintained separately from Compose
- [X] T013 [P] [US1] Add the same nine topics to the `topic-init` chain in `docker-compose.kafka.yml`, keeping it in step with T012
- [X] T014 [US1] Run `sbt exampleRun` against the local stack and **record which examples fail and how**. Expect six failures per R3. This is the red state the corrections are written against — do not skip recording it

### Correct the examples (green)

- [X] T015 [P] [US1] Correct `src/test/java/org/galaxio/gatling/kafka/javaapi/examples/ProducerSimulation.java`: add the missing `setUp(...)` with the protocol it already declares, retopic to `ex.java.producer.t`, add assertions for 3 requests / 100% success (FR-002a; DSL calls and order unchanged per FR-002b)
- [X] T016 [P] [US1] Correct `src/test/java/org/galaxio/gatling/kafka/javaapi/examples/AvroClassWithRequestReplySimulation.java`: replace the `"schRegUrl"` literal with `http://localhost:9094`, replace the empty `MyAvroClass` with a real Avro type, retopic to `ex.java.avrorr.t`, add assertions for 1 request / 100% success
- [X] T017 [P] [US1] Correct `src/test/scala/org/galaxio/gatling/kafka/examples/AvroClassWithRequestReplySimulation.scala`: same two defects as T016 — `"schRegUrl".split(',')` and the fieldless `case class MyAvroClass()` — retopic to `ex.scala.avrorr.t`, add assertions
- [X] T018 [P] [US1] Correct `src/test/scala/org/galaxio/gatling/kafka/examples/BasicSimulation.scala`: make the second exchange echo on one topic instead of `myTopic2` → `test.t1`, fix `jsonPath("$.M").is("DKF")` to match the `{"m":"dkf"}` body it actually sends, reduce `atOnceUsers(50)` to the smallest volume the assertions need (DR-3), retopic to `ex.scala.basic.t`
- [X] T019 [P] [US1] Retopic and add assertions to the four examples needing no structural correction: `javaapi/examples/BasicSimulation.java` (`ex.java.basic.t`), `javaapi/examples/MatchSimulation.java` (`ex.java.match.t`), `examples/MatchSimulation.scala` (`ex.scala.match.t`), `examples/ProducerSimulation.scala` (`ex.scala.producer.t`)
- [X] T020 [P] [US1] Retopic and add assertions to `src/test/scala/org/galaxio/gatling/kafka/examples/Avro4sSimulation.scala` (`ex.scala.avro4s.t`, 2 requests / 100% success)
- [X] T021 [US1] Keep both `MatchSimulation`s at one user in flight and assert to that bound — their `matchByMessage` returns a constant, so any reply matches any request (DR-3, contract C5). Do not raise the profile
- [X] T022 [US1] Run `sbt exampleRun` against the local stack; all nine pass. Compare against the T014 record to confirm every observed failure is accounted for

### Wire into CI

- [X] T023 [US1] Add the example-run step to `.github/workflows/ci.yml` after the smoke-validation step and before the coverage step, and confirm the existing three `Gatling / testOnly` simulations are unchanged (FR-014, SC-008)

**Checkpoint**: Nine published JVM examples run green in CI. SC-002 met (9 of 9, up from 0).

---

## Phase 4: User Story 2 — The gate asserts what the project says it asserts (Priority: P1)

**Goal**: `ExampleSmokeValidation` really constructs every example, and every statement describing it
is true.

**Independent Test**: Break one example's protocol or scenario construction in a way that still
compiles and still leaves a no-arg constructor; confirm the gate fails. Independent of US1 and US3.

### Red first

- [X] T024 [US2] Introduce a construction-time break in one example that compiles cleanly (e.g. a required protocol setting removed), run `sbt "Test / runMain …ExampleSmokeValidation"`, and **record that it passes**. This is the defect being fixed — evidence, not a formality
- [X] T025 [US2] Leave the break in place while implementing T026–T028; the gate must go red before the break is reverted

### Implementation

- [X] T026 [US2] In `src/test/scala/org/galaxio/gatling/kafka/examples/ExampleSmokeValidation.scala`, replace `clazz.getDeclaredConstructor()` with `clazz.getDeclaredConstructor().newInstance()` so field initialisers execute (FR-009, contract C4)
- [X] T027 [US2] Replace the hand-written nine-FQCN list in `ExampleSmokeValidation.scala` with the derived set from `ExampleInventory` (T005–T007), so an example on disk but absent from coverage fails (FR-005, contract C3)
- [X] T028 [US2] Make the gate's failure message name the offending example and the construction failure (FR-010)
- [X] T029 [US2] Confirm the gate now fails on the T024 break, then revert the break and confirm green (SC-005, quickstart Drill 4)
- [X] T030 [US2] Confirm the gate passes with the broker and registry stopped — `docker compose -f docker-compose.kafka.yml down` then re-run (FR-011, contract C4). Any example that needs a live service falls to FR-004 with the reason recorded

### Documentation correction (lands after the things it describes exist)

- [X] T031 [P] [US2] Correct `AGENTS.md` Test Model: `ExampleSmokeValidation` constructs (now true), CI runs three test simulations not two, and the nine examples now run — state that compilation is what protects an example from an API break where that is the case (FR-012, FR-013)
- [X] T032 [P] [US2] Correct the Examples section of `README.md`: say what the gate asserts, and document `sbt exampleRun` and `scripts/check-kotlin-examples.sh` alongside it
- [X] T033 [US2] Amend `.specify/memory/constitution.md` to 1.0.1 (PATCH per R8): correct the Development Workflow "Full CI gate" paragraph to name all three test simulations plus the example runs; leave Principle I's `ExampleSmokeValidation` clause unchanged — T026 makes it true. Update the Sync Impact Report at the top per the constitution's own amendment procedure
- [X] T034 [US2] Re-read every statement about the gate in `README.md`, `AGENTS.md`, and the constitution against what now exists; zero overstatements (SC-006, contract C7)

**Checkpoint**: A DSL break in any example is caught. No document overstates coverage.

---

## Phase 5: User Story 3 — Kotlin examples cannot rot unnoticed (Priority: P2)

**Goal**: Every Kotlin example compiles against this build on every CI run.

**Independent Test**: Introduce a syntax error into one Kotlin example, confirm the Kotlin job fails,
revert, confirm it passes. Independent of US1 and US2.

**⚠️ Blocked on approval**: CI gains a Kotlin compiler it does not have today — see the Constraints
gate in [plan.md](./plan.md) and Complexity Tracking. Do not start T037 before that is settled.

- [X] T035 [US3] Add an `exampleClasspath` task to `build.sbt` writing `Test / fullClasspath` to `target/example-classpath.txt`, so the Kotlin examples compile against the classes this build produced rather than a published release (FR-003b)
- [X] T036 [US3] Create `scripts/check-kotlin-examples.sh`: discover `src/test/kotlin/**/*.kt` by glob (never a hard-coded list — US3 acceptance 3), invoke `sbt exampleClasspath`, compile with `kotlinc -classpath … -d <temp>`, exit non-zero on any failure
- [X] T037 [US3] Make the script exit non-zero with a clear message when `kotlinc` is absent (contract C2 — a check that skips when its tool is missing is the defect this feature exists to fix)
- [X] T038 [US3] Run the script locally and **record whether the four Kotlin examples still compile**. Nothing has compiled them since #181; the answer decides how much T039 carries
- [X] T039 [US3] Correct any Kotlin example that fails to compile against the current API, keeping what it teaches intact (FR-002b)
- [X] T040 [US3] Add the Kotlin compile step to `.github/workflows/ci.yml`, including provisioning the compiler — a pinned JetBrains release archive verified by checksum, per the approved decision from the Constraints gate

**Checkpoint**: 4 of 4 Kotlin examples compiled by CI, up from 0 (SC-003).

---

## Phase 6: Polish & Acceptance

**Purpose**: Produce the evidence that the coverage claimed by this feature is real. Per clarification
Q4 the drills are one-off acceptance artifacts, not standing CI checks — so if they are not recorded
here, they do not exist.

- [X] T041 [P] **Drill 1 (Scala, run-time defect)**: point one covered Scala example's reply topic at a topic nothing produces to; `sbt exampleRun` goes red on the success assertion; revert. Record the example, the defect, and the red job (FR-007a, FR-007b)
- [X] T042 [P] **Drill 2 (Java, run-time defect)**: reduce one covered Java example's injection to zero users, leaving the scenario intact; `sbt exampleRun` goes red on the request-count assertion — the "sends nothing" case (SC-005a); revert and record
- [X] T043 [P] **Drill 3 (Kotlin, compile-time defect)**: introduce a syntax error in one Kotlin example; `scripts/check-kotlin-examples.sh` exits non-zero naming the file; revert and record
- [X] T044 **Drill 5 (uncovered example detected)**: add a stray simulation file to the Java examples directory; the gate fails because it has no coverage level; delete and confirm green (SC-007, DR-5)
- [X] T045 Confirm no two covered examples share a topic and every topic exists in both `.github/workflows/ci.yml` and `docker-compose.kafka.yml` (contract C6)
- [X] T046 Run the full local gate from [quickstart.md](./quickstart.md) §4 end to end and confirm the existing three test simulations run unchanged (FR-014, SC-008)
- [X] T047 Run `sbt scalafmtAll scalafmtSbt` then `sbt scalafmtCheckAll scalafmtSbtCheck compile test` — every commit must be green on its own (Principle V)
- [ ] T048 Open the PRs per the delivery slices in [plan.md](./plan.md), each single-concern, each assigned to milestone **v1.13.0 Test suite integrity**, and close #240 from the last one to land (`Closes #240`). Verify with `scripts/check-linkage.sh --pr <N>`

---

## Dependencies

```text
Phase 1 (Setup: T001–T004)
        │
        ▼
Phase 2 (Foundational: T005–T007)  ← blocks BOTH P1 stories
        │
        ├──────────────────────────┬───────────────────────────┐
        ▼                          ▼                           ▼
Phase 3 US1 (T008–T023)    Phase 4 US2 (T024–T030)     Phase 5 US3 (T035–T039)
   JVM examples run          gate constructs              Kotlin compiles
        │                          │                           │
        │                          ▼                           │
        │                  US2 docs (T031–T034)                │
        │                  ← needs US1 + US3 to exist          │
        │                     before describing them           │
        └──────────────────────────┴───────────────────────────┘
                                   ▼
                        Phase 6 Acceptance (T041–T048)
```

**Story independence**: US1, US2 (implementation half) and US3 are independent once Phase 2 is done —
different files, different mechanisms. Only US2's documentation half (T031–T034) depends on the other
two, deliberately: correcting a claim before its subject exists makes the documentation wrong in the
other direction.

**Hard sequencing inside US1**: T014 (observe the six failures) must precede T015–T021 (correct them).
This is the red-before-green step Principle IV requires, and skipping it means the corrections are
written against the plan's description of the defects rather than against the defects.

**External block**: T037 and T040 wait on the Constraints-gate approval for the CI Kotlin compiler.

---

## Parallel Opportunities

- **T012 ‖ T013** — the two broker definitions are separate files, maintained separately by design
- **T015 ‖ T016 ‖ T017 ‖ T018 ‖ T019 ‖ T020** — six distinct example files, no shared state
- **T031 ‖ T032** — `AGENTS.md` and `README.md` are independent
- **T041 ‖ T042 ‖ T043** — one drill per language, different files and different commands
- **US1 ‖ US2-implementation ‖ US3** — three independent workstreams after Phase 2

---

## Implementation Strategy

**MVP = Phase 1 + Phase 2 + Phase 3 (US1)**: the nine published JVM examples run against a real
broker. This alone closes the gap #240 is about for the language with the largest user population,
and is independently shippable.

**Increment 2 = Phase 4 (US2)**: the gate stops lying, and so do the three documents describing it.
Cheap, independent, and the one that fixes a defect in the project's own governance.

**Increment 3 = Phase 5 (US3)**: Kotlin. Last because it is blocked on toolchain approval, not
because it matters least — the defect that motivated #240 was a Kotlin one.

**Phase 6 is not optional.** The drills are the only evidence that any of this works. Clarification
Q4 chose one-off manual drills over a standing automated guard precisely on the understanding that
they would be recorded; unrecorded, this feature makes exactly the kind of unverified coverage claim
it exists to remove.

---

## Phase 7: Native run mechanisms (2026-08-20)

**Why**: `exampleRun` was rejected on review — three bespoke entities where every build system already
has exactly one command, and none of them something a consumer of this plugin could copy. The
research behind the replacement is recorded in [research.md](./research.md) R1 (superseded section).

- [X] T049 Enumerate every key gatling-sbt declares (15) from bytecode and confirm none runs a simulation by class name; execution is delegated wholly to `Defaults.testTasks`
- [X] T050 Measure `Test/runMain io.gatling.app.Gatling` and reject it: `Gatling$.main` calls `sys.exit`, so two chained invocations exited 0 having run only the first — a deliberately failing second simulation never ran
- [X] T051 Build the Gradle option to settle it empirically: `io.gatling.gradle` 3.13.5.4 cannot configure on Gradle 9.4.1 (`Could not get unknown property 'reportsDir'` — the Convention API removed in 9.0); only 3.15.1.2 pinned back to Gatling 3.13.5 works, a pairing Gatling never tests
- [X] T052 Delete `exampleRun`, `exampleClasspath`, `ExampleSimulationRunner.scala` and `scripts/check-kotlin-examples.sh`; trim `GatlingInternals` to `installTestConfiguration()`
- [X] T053 Run the Scala examples under `sbt Gatling/test` with `Gatling / parallelExecution := false`; collapse the three hand-listed `testOnly` invocations in `.github/workflows/ci.yml` into one
- [X] T054 Create `examples/maven` — a consumer project depending on the published artifact, with gatling-maven-plugin 4.19.1 and kotlin-maven-plugin 2.4.10
- [X] T055 Move the Java examples there and rewrite the four Kotlin ones, which had never compiled: an unclosed `.exec(`, missing io.confluent and kafka serialization imports, a `MyAvroClass` that exists only in the Scala example, and the `.topic()` removed in 1.0.0
- [X] T056 Give the two sets distinct packages (`org.galaxio.examples.javaapi`, `…kotlinapi`): identical FQCNs would overwrite each other in one module's `test-classes`
- [X] T057 Add the four `ex.kotlin.*` topics to both broker definitions and re-verify all 13 are in each
- [X] T058 Wire CI: `sbt publishM2` under a sentinel version, then `mvn -B -f examples/maven/pom.xml verify`
- [X] T059 Point `ExampleInventory` at both projects so an example added anywhere without coverage still fails; `ExampleSmokeValidation` now constructs the Scala examples and reports the rest as covered by `examples/maven`
- [X] T060 Correct `README.md`, `AGENTS.md` and the constitution to describe the mechanisms that now exist
- [X] T061 Re-run the drills that changed hands: Kotlin compile-time defect now fails `mvn test-compile` naming the file (Drill 3, previously unrunnable)

**Read-back**: `sbt Gatling/test` — 8 Scala simulations, exit 0. `mvn -f examples/maven/pom.xml verify`
— 4 Java + 4 Kotlin, exit 0, no warnings. `sbt scalafmtCheckAll scalafmtSbtCheck compile
ExampleSmokeValidation test` — 84/84, gate green with the whole stack stopped.

**Still open**: T048 — PRs, milestone assignment and `Closes #240`.

---

## Phase 8: One consumer project per language (2026-08-21)

**Why**: Phase 7 left Java and Kotlin sharing a single Maven module, which forced distinct packages so
that identical simple names would not overwrite each other in one `test-classes`. Three projects, one
per language on the build tool its users use, removes that workaround instead of documenting it — and
is the shape Gatling ships its own demo projects in.

- [X] T062 Split `examples/maven` into `examples/java` (Maven) and `examples/kotlin` (Gradle); strip the Kotlin plugin from the Java pom
- [X] T063 Create `examples/scala` (sbt) and move the five Scala examples plus `GatlingRunner` out of the plugin's `src/test/scala`, repackaged to `org.galaxio.examples.scalaapi`
- [X] T064 Commit a Gradle wrapper pinned to 8.12: `io.gatling.gradle` 3.13.5.4 matches Gatling 3.13.5 and cannot configure on Gradle 9 (`Project#javaexec` and the Convention API, both removed in 9.0)
- [X] T065 Rename `ExampleSmokeValidation` to `ExampleCoverageCheck` — it can no longer construct anything, since no example is on the plugin's classpath. It keeps C3 and C6, both of which need no broker
- [X] T066 Repoint `ExampleInventory` at the three projects; shrink `notExamples` to the one entry that still applies
- [X] T067 Rewire CI: one step per example project, each with its own `working-directory`
- [X] T068 Amend the constitution to 1.1.0 (MINOR) — Principle I now requires every example to be compiled and run from a consumer project, where it previously asked only that a gate construct them
- [X] T069 Correct `README.md`, `AGENTS.md`, the spec-kit templates, `build.sbt` comments and the contracts to describe the three projects
- [X] T070 Assign PR #244 to `v1.13.0 Test suite integrity`, the milestone issue #240 lives in; `scripts/check-linkage.sh --pr 244` passes

**Read-back**: coverage contract green (13 examples, 13 topics, 3 projects); `sbt "Gatling / test"` in
the plugin runs exactly the 3 harnesses; `examples/scala` 5, `examples/java` 4, `examples/kotlin` 4.
