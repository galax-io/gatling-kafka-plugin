---

description: "Task list for 005-classpath-dependency-shedding"
---

# Tasks: Classpath and Dependency Shedding

**Input**: Design documents from `/specs/005-classpath-dependency-shedding/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Mandatory per Constitution Principle IV. This feature changes observable behavior — what a
consumer's build resolves and whether the DSL loads without vendor artifacts — so every behavior task
is preceded by a test written to fail first. Two tests fail against today's code for the exact reason
this feature exists: the POM check (Contract C1) and the classpath-isolation spec (Contract E1). Per
Principle II, the produce and request-reply verification runs against a real broker.

**Organization**: Tasks are grouped by user story. Note that stories and *commits* are not the same
partition — see the mapping below.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1–US4)
- Include exact file paths in descriptions

## Path Conventions

Single-module Scala/sbt project:

- **Scala plugin sources**: `src/main/scala/org/galaxio/gatling/kafka/`
- **Java facade**: `src/main/java/org/galaxio/gatling/kafka/javaapi/`
- **Tests**: `src/test/{scala,java,kotlin}/`
- **Build/dependency truth**: `build.sbt`, `project/Dependencies.scala`

## Story → Commit Mapping

Principle V requires one tracked issue per semantic commit, and the story partition does not line up
with it. Implement by story; **commit** by this table.

| Commit | Issue | Tasks |
| --- | --- | --- |
| `docs(speckit): add 005-classpath-dependency-shedding spec/plan/tasks` | — | spec artifacts, lands first |
| `fix(build): resolve every inherited dependency from Maven Central (#NEW)` | companion issue (T001) | T004–T009 |
| `fix(build): make Confluent Avro support opt-in (#185)` | #185 | T010–T031 |
| `refactor(request): deprecate the unused Kafka Streams surface (#214)` | #214 | T032–T038 |

Each commit must be green on its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`.

---

## Phase 1: Setup

**Purpose**: Establish tracking and confirm the defect still reproduces before changing anything.

- [ ] T001 Open a companion GitHub issue for the Kafka client relocation (or extend the scope of #185), assign it to milestone 10, and record its number in the Story → Commit Mapping table above and in the Issue Decomposition table in `specs/005-classpath-dependency-shedding/plan.md`
- [ ] T002 [P] Confirm the baseline per [quickstart.md](./quickstart.md) Step 0: fetch the published `1.2.0` POM, verify four inherited dependencies and no `<repositories>` element
- [ ] T003 [P] Re-probe the vendor coordinates currently declared in `project/Dependencies.scala` against Maven Central, using whatever versions dependency automation has landed since specification; a `200` on any `-ce`/`-ccs` coordinate is a new finding that invalidates R1 and must stop the work

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The published-metadata gate, the coordinate relocation, and the two verification gates
from plan.md. Nothing downstream produces trustworthy evidence until G1 closes.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T004 Add a failing-first sbt check task in `build.sbt` asserting Contracts C1–C3 over `makePom` output: no inherited-scope dependency may match `io.confluent:*` or `org.apache.kafka:*` with a `-ce`/`-ccs` version suffix. Use an offline pattern deny-list, not a network probe, so a network failure cannot read as a pass. Confirm it **FAILS** naming all four coordinates
- [ ] T005 Extend the same check in `build.sbt` with Contract C5: `groupId`, `artifactId`, `packaging`, `licenses`, `scm`, `developers`, `organization`, `url`, and the `provided`/`test` status of the Gatling and testing artifacts are unchanged
- [ ] T006 Relocate `kafka-clients` and `kafka-streams-scala` from `7.9.9-ce` to Apache `3.9.2` in `project/Dependencies.scala`
- [ ] T007 Close Gate G1: stop the Confluent client from evicting the Apache one via `dependencyOverrides` or an exclusion on the transitive `kafka-schema-registry-client` path, in `project/Dependencies.scala` / `build.sbt`
- [ ] T008 Verify Gate G1 with `sbt -batch "clean; compile; Test/compile; evicted"` — pass requires exit 0 **and** no `org.apache.kafka:kafka-clients` conflict in the `evicted` report. Record the outcome in the Verification Gates table in `plan.md`
- [ ] T009 Close Gate G2: compile a plain Java simulation and a plain Kotlin simulation against `target/scala-2.13/classes` with all `io.confluent` jars excluded from the classpath, exercising `KafkaDsl.avro(JExpression, Serializer, Deserializer)` so overload resolution is forced. Record the verdict in `research.md` R3 and in `plan.md`. **STOP and escalate to the maintainer if it fails** — the `SchemaRegistryClient`-typed overloads would have to move, which is a Java-source break with no deprecation window

**Checkpoint**: The build declares Central-resolvable Kafka coordinates, compiles and tests against the
same client it ships, and the Java facade's constraint is known rather than assumed.

---

## Phase 3: User Story 1 - A new consumer can install and run the plugin (Priority: P1) 🎯 MVP

**Goal**: A consumer whose build resolves from Maven Central only can declare the plugin and nothing
else, then compile and run plain produce and request-reply simulations.

**Independent Test**: From a scratch project on each of sbt, Gradle, and Maven — configured with Maven
Central and the local repository only, no Confluent resolver — declare only the plugin, then resolve,
compile, and execute a plain produce simulation and a plain request-reply simulation against a broker.

### Tests for User Story 1 (MANDATORY — Principle IV) ⚠️

> Write these first and confirm they FAIL before any implementation task in this phase.

- [ ] T010 [P] [US1] Failing-first classloader spec in `src/test/scala/org/galaxio/gatling/kafka/classpath/PlainClasspathIsolationSpec.scala`: assert `org.galaxio.gatling.kafka.Predef` and `org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks` both initialise under a classloader that refuses `io.confluent.*`. Must fail today with `NoClassDefFoundError: io/confluent/kafka/streams/serdes/avro/GenericAvroSerde`
- [ ] T011 [P] [US1] Guard `src/test/scala/org/galaxio/gatling/kafka/classpath/PlainClasspathIsolationSpec.scala` against a vacuous pass: it must fail loudly if `GenericAvroSerde` *is* loadable through the restricted classloader, so a misconfigured harness reports itself instead of reporting success
- [ ] T012 [P] [US1] Create the consumer-resolution harness in `scripts/consumer-resolution/` — minimal sbt, Gradle, and Maven fixture projects configured with Maven Central plus the local repository only, each declaring nothing but the plugin and containing the README's minimal produce simulation. Confirm all three **FAIL** to resolve today

### Implementation for User Story 1

- [ ] T013 [US1] Create `src/main/scala/org/galaxio/gatling/kafka/confluent.scala` — an `object confluent` holding `avroSerde` and `serdeClass[T]`, mirroring the existing `object avro4s` in `src/main/scala/org/galaxio/gatling/kafka/avro4s.scala`. This becomes the only place in `src/main` that constructs a Confluent serde
- [ ] T014 [US1] In `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala`, replace `implicit val avroSerde = new GenericAvroSerde()` with `implicit lazy val avroSerde: Serde[GenericRecord] = confluent.avroSerde`, and delegate `serdeClass[T]` the same way. `lazy` is required: without it, `Predef` initialisation would force `object confluent` to initialise and reconstruct the serde one hop later. Remove the now-unused `io.confluent` imports
- [ ] T015 [US1] In `src/main/java/org/galaxio/gatling/kafka/javaapi/checks/KafkaChecks.scala`, replace `val avroSerde = new GenericAvroSerde()` with a lazy delegation to `confluent.avroSerde` and remove the `io.confluent` import
- [ ] T016 [US1] Move `avroSerdes` and `avroSerializers` to `provided` scope in `project/Dependencies.scala`
- [ ] T017 [US1] Verify T010–T011 in `src/test/scala/org/galaxio/gatling/kafka/classpath/PlainClasspathIsolationSpec.scala` now pass, and that no file reachable from `Predef` names a Confluent type in a signature or in bytecode (Contract E1)
- [ ] T018 [US1] Verify the T004–T005 check in `build.sbt` now passes (Contracts C1–C3, C5)
- [ ] T019 [US1] Verify T012: all three scratch projects resolve and compile. Then execute the plain produce and plain request-reply simulations from the sbt fixture against the `docker-compose.kafka.yml` broker, satisfying Contract E1 items 3 and 4
- [ ] T020 [US1] Wire the harness into `.github/workflows/ci.yml` as a job that runs `sbt publishM2` and then all three fixture checks
- [ ] T021 [US1] Update the Installation section of `README.md` with plain install snippets for sbt, Gradle, and Maven that require no additional repository

**Checkpoint**: A Central-only consumer can install the plugin and run plain simulations. This is the
MVP and closes the S1 defect.

---

## Phase 4: User Story 2 - Avro / Schema Registry has a documented, working path (Priority: P2)

**Goal**: A consumer needing Schema-Registry-backed Avro adds exactly what the documentation names and
their simulation compiles and runs, with behavior identical to before.

**Independent Test**: From a scratch project configured with Maven Central plus exactly the repository
the README names, declaring the plugin plus exactly the coordinates the README names, compile and run
an Avro produce simulation and an Avro body check against the broker and Schema Registry.

### Tests for User Story 2 (MANDATORY — Principle IV) ⚠️

- [ ] T022 [P] [US2] Add an Avro variant to `scripts/consumer-resolution/` — Confluent repository plus the two opt-in coordinates with the Kafka broker artifact excluded (R9) — compiling and running an Avro produce simulation and an Avro body check against the Compose stack. Confirm it fails before the opt-in entry point exists

### Implementation for User Story 2

- [ ] T023 [US2] Move every in-project use of the deprecated Avro members to `import org.galaxio.gatling.kafka.confluent._` across `src/test/scala/`, `src/test/java/`, and `src/test/kotlin/`. Required by `-Xfatal-warnings`, and it makes the examples demonstrate the documented consumer path
- [ ] T024 [US2] Apply the Gate G2 verdict to the Java facade in `src/main/java/org/galaxio/gatling/kafka/javaapi/KafkaDsl.java` and `src/main/java/org/galaxio/gatling/kafka/javaapi/expressions/Builders.java`: if G2 passed, annotate the `SchemaRegistryClient`-typed entry points `@Deprecated` naming their opt-in replacement and leave them in place; if G2 failed, do not proceed without the maintainer decision from T009
- [ ] T025 [US2] Document the Avro opt-in path in the Avro Support section of `README.md`: exact coordinates, the exact Confluent repository URL, the broker exclusion, and the opt-in import — expressed for sbt, Gradle, and Maven (FR-008)
- [ ] T026 [US2] Verify the Avro fixture in `scripts/consumer-resolution/` (T022) passes, and that every Avro and Schema Registry capability available before this change behaves identically (FR-009), including the Java and Kotlin Avro entry points

**Checkpoint**: The opt-in Avro path works end to end and is fully documented.

---

## Phase 5: User Story 3 - An upgrading consumer is told exactly what changed (Priority: P2)

**Goal**: Existing suites upgrade with no surprises — plain suites need nothing, Avro suites need only
the documented steps.

**Independent Test**: Upgrade a 1.2.x suite by changing only the plugin version, apply exactly the
migration guide's steps, and build.

### Tests for User Story 3 (MANDATORY — Principle IV) ⚠️

- [ ] T027 [P] [US3] Verify SC-006 per [quickstart.md](./quickstart.md) Step 8: a plain-serialization suite written against 1.2.x compiles and runs after changing only the plugin version, with zero edits to build files or sources
- [ ] T028 [P] [US3] Verify a Schema Registry Avro suite written against 1.2.x compiles and runs after applying only the steps listed in the Migration Guide of `README.md`. Any change beyond those steps means FR-011 is unmet

### Implementation for User Story 3

- [ ] T029 [US3] Add a Migration Guide entry to `README.md` stating which artifacts are no longer inherited, exactly what to add to restore each capability, which repository to configure, and the one-line import change for Schema Registry Avro users
- [ ] T030 [US3] Add a Kafka client column to the Compatibility table in `README.md` reflecting the relocated version line (FR-013)
- [ ] T031 [US3] Verify every deprecation in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala` and `src/main/java/org/galaxio/gatling/kafka/javaapi/KafkaDsl.java` names both its replacement (or, for the Kafka Streams implicits, the absence of one) and its removal release, and that a consumer compiling with warnings-as-errors is forewarned by the migration entry

**Checkpoint**: Upgrading is mechanical and documented for every consumer population.

---

## Phase 6: User Story 4 - Every inherited dependency is justified (Priority: P3)

**Goal**: Nothing is inherited without a reason, and the dead weight that remains is marked with a
removal release.

**Independent Test**: Enumerate the inherited dependencies; each must trace to a plugin code path or to
a recorded deprecation. Confirm no build declaration exists that the build never applies.

### Tests for User Story 4 (MANDATORY — Principle IV) ⚠️

- [ ] T032 [P] [US4] Extend the `build.sbt` check with Contract C3 and data-model rule DR-4: every inherited dependency traces to a code path or a recorded deprecation, and **at most one** may be justified by deprecation. Confirm it fails before T033–T035 land

### Implementation for User Story 4

- [ ] T033 [P] [US4] Annotate `sessionWindowedSerde` and `consumedFromSerde` in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala` as `@deprecated(..., "1.3.0")`, each stating that the plugin does not use it and that Kafka Streams users should depend on the artifact directly. Do **not** name a replacement — none exists, and inventing one would imply a capability this plugin does not offer
- [ ] T034 [P] [US4] Remove the `avroCompiler` declaration from `project/Dependencies.scala` — the build never adds it to `libraryDependencies`, yet dependency automation has been maintaining it (FR-015)
- [ ] T035 [P] [US4] Remove the unused Kafka Streams imports at `src/test/scala/org/galaxio/gatling/kafka/examples/BasicSimulation.scala:11-12` (FR-016)
- [ ] T036 [US4] Review the `.exclude("org.apache.kafka", "kafka-streams-scala")` on `avroSerdes` in `project/Dependencies.scala` (FR-017): remove it if the relocation and scope change dissolved the collision, otherwise state in a comment which collision it still prevents
- [ ] T037 [US4] Verify no plugin code path calls either implicit deprecated in `src/main/scala/org/galaxio/gatling/kafka/request/KafkaSerdesImplicits.scala`, and that `kafka-streams-scala` remains inherited and Central-resolvable as the single recorded exception under DR-4
- [ ] T038 [US4] Verify the Contract C3 / DR-4 check added to `build.sbt` in T032 passes

**Checkpoint**: The dependency set is justified end to end, with exactly one documented exception.

---

## Phase 7: Polish & Cross-Cutting Concerns

- [ ] T039 Run `sbt scalafmtAll scalafmtSbt`, then `sbt scalafmtCheckAll scalafmtSbtCheck compile test`
- [ ] T040 Run `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"` (API-compat gate, Principle I)
- [ ] T041 Run the full CI gate: `sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport`. Contract E5 and SC-009 require this to be green with **no** suite relaxed, skipped, retried, or disabled to accommodate the dependency change — a modified suite is a finding, not a pass
- [ ] T042 [P] Walk [quickstart.md](./quickstart.md) Steps 0–8 end to end and confirm each "must fail before the fix" row actually did
- [ ] T043 [P] Update the Verification Gates table in `plan.md` with the G1 and G2 outcomes, and the Complexity Tracking table if the G2 verdict changed the compatibility story
- [ ] T044 Confirm the commits match the Story → Commit Mapping table in `specs/005-classpath-dependency-shedding/tasks.md`, each carrying milestone 10 and `Closes #NNN`, with no spec artifacts folded into an implementation commit (Principle V)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies
- **Foundational (Phase 2)**: depends on Setup — **BLOCKS all user stories**. T008 (G1) in particular gates the trustworthiness of every later test result; T009 (G2) gates the Java design in US2
- **US1 (Phase 3)**: depends on Phase 2
- **US2 (Phase 4)**: depends on US1 — the opt-in object created in T013 is what US2 documents and tests
- **US3 (Phase 5)**: depends on US1 and US2 — the migration guide describes both halves
- **US4 (Phase 6)**: depends on Phase 2 only; independent of US1–US3 and can run in parallel with them
- **Polish (Phase 7)**: depends on all stories

### Story Independence

US4 is genuinely independent and could ship alone. US2 and US3 are not independent of US1 in this
feature: US1 creates the opt-in entry point that US2 documents, and US3 documents the upgrade path
across both. This is a deliberate departure from the usual one-story-per-slice rule — the stories
partition the *outcome*, while the commits partition the *scope*, which is what the Story → Commit
Mapping exists to reconcile.

### Within Each Story

Tests are written and confirmed failing before implementation (Principle IV). The only tasks exempt are
the pure-mechanical ones in Phase 6 (T034, T035), which remove declarations and imports the build never
uses — demonstrable as behavior-neutral by the existing suite passing unchanged.

### Parallel Opportunities

- T002, T003 in Setup
- T010, T011, T012 — the three US1 test tasks, different files
- T033, T034, T035 — different files, no ordering between them
- T042, T043 in Polish
- **Phase 6 (US4) can run in parallel with Phases 3–5** once Phase 2 completes, with one caveat: T036
  touches `project/Dependencies.scala`, which T016 also edits — sequence those two

---

## Parallel Example: User Story 1 Tests

```bash
# Launch the three failing-first US1 test tasks together (different files):
Task: "Classloader spec in src/test/scala/org/galaxio/gatling/kafka/classpath/PlainClasspathIsolationSpec.scala"
Task: "Vacuous-pass guard for the same spec"
Task: "Consumer-resolution fixtures in scripts/consumer-resolution/ for sbt, Gradle, Maven"
```

---

## Implementation Strategy

### MVP (User Story 1)

1. Phase 1: Setup — open the companion issue, confirm the baseline
2. Phase 2: Foundational — **critical**, and do not treat any later result as evidence until T008 passes
3. Phase 3: US1
4. **STOP and VALIDATE**: a Central-only consumer installs and runs plain simulations
5. The S1 defect is closed at this point; everything after is completeness

### Incremental Delivery

1. Setup + Foundational → the build declares and resolves what it ships
2. US1 → plain consumers work from Central alone (**MVP**)
3. US2 → Avro consumers have a documented, working opt-in path
4. US3 → upgrading is mechanical for every population
5. US4 → dependency set fully justified

### Sequencing Note

The single highest-risk item is **T009 (Gate G2)**. It sits in Phase 2 deliberately: if the Java facade
cannot compile without Schema Registry on the classpath, the `SchemaRegistryClient`-typed overloads
must move, which is a Java-source break with no deprecation window and a possible change to the
release's version number. Discovering that in Phase 4 would mean reworking US1's design decisions
after they shipped.

---

## Notes

- [P] = different files, no dependencies
- Commit per tracked issue, not per task — see the Story → Commit Mapping
- Verify tests fail before implementing; a test that passes before the fix is a broken test
- Version strings in these tasks (`7.9.9-ce`, `3.9.2`) are a snapshot of `main` at `4516572`.
  Dependency automation moves them; the tasks are written against dependency *properties*, so a bump
  changes the numbers and nothing else
