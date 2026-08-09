# Implementation Plan: Classpath and Dependency Shedding

**Branch**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/005-classpath-dependency-shedding/spec.md`

**Milestone**: [v1.3.0 Classpath and dependency shedding](https://github.com/galax-io/gatling-kafka-plugin/milestone/10)

## Summary

The published artifact declares four dependencies in a scope consumers inherit that do not exist on
Maven Central, and its POM carries no repository from which to fetch them, so the documented
installation path fails for every new consumer. The fix has two halves.

**Relocate what can be relocated.** The Kafka client and Kafka Streams Scala artifacts are pinned to
Confluent's vendor rebuild (`7.9.9-ce` on current `main`); the equivalent Apache releases are on Maven
Central. Moving to Apache `3.9.2` — the newest 3.9.x on Central, the same upstream code Confluent
Platform 7.9.x is built from, and the match for the `cp-kafka:7.9.5` brokers this project tests
against — makes both resolvable with no functional change. Two compile spikes, one at the branch point
and one against current `main`, confirmed source compatibility with zero warnings under
`-Xfatal-warnings`. Both also revealed that the build silently resolves back to the Confluent client
via a transitive path, so the version must be pinned before that result means anything.

Note that the vendor coordinates moved (`7.9.5-ce`/`7.9.8` → `7.9.9-ce`/`7.9.9`) while this feature was
being specified, with the defect untouched. Nothing asserts Central-resolvability, so routine
dependency automation keeps the broken coordinates current and green — which is why the fix ships with
a standing assertion rather than only a corrected version string.

**Stop inheriting what cannot be relocated.** The two `io.confluent` Avro artifacts have no Maven
Central equivalent at any version, so they must become opt-in. The obstacle is that the coupling is not
confined to the Avro feature: `Predef` eagerly constructs a `GenericAvroSerde` at initialisation, so
every simulation touches Confluent whether or not it uses Avro. The declared types in those members are
already Confluent-free (`Serde[T]`, `GenericRecord` — both on Central); only the initialiser
expressions are not. The Confluent constructions move to the opt-in object and the members left behind
become lazy delegations to it — one keyword and one annotation each. That decouples the default DSL
entry point while leaving every published signature in place, which is what lets the deprecation window
required by Principle I coexist with the S1 fix in the same release.

**On scope.** A simpler fix exists and was weighed: document the Confluent resolver in the README and
change nothing else. It works, and it closes the S1 defect. It was rejected because it makes every
consumer add a third-party repository to use a feature only some of them want — the plugin already
treats `avro4s` as opt-in for exactly this reason, and these two artifacts are the inconsistency. The
relocation of the Kafka client is not a workaround in either scenario: the plugin uses no vendor API, so
the `-ce` pin is the accidental thing being removed.

The unused Kafka Streams surface is deprecated rather than removed, and its artifact stays — its types
appear in implicit *signatures*, so it cannot be shed while the implicits must keep compiling.

## Technical Context

**Language/Version**: Scala 2.13.18 on sbt 1.12.15; Java 17+ (Temurin in CI); Java facade sources
compiled in the same module

**Primary Dependencies**: Gatling 3.13.5 (`provided`); Apache Kafka clients — relocating from
`org.apache.kafka:kafka-clients:7.9.9-ce` to Maven Central `3.9.2`; Confluent Avro serialization and
Schema Registry client `7.9.9` — moving from inherited to opt-in; avro4s 4.1.2 and
`org.apache.avro:avro` 1.12.1 (already correct). Versions are as of `main` at `4516572`; the plan is
written against dependency *properties*, so a bump before implementation invalidates no decision

**Storage**: N/A

**Testing**: MUnit + ScalaTest unit specs; Testcontainers (`confluentinc/cp-kafka:7.9.5`) integration
specs; Gatling simulations (`KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`) against the
`docker-compose.kafka.yml` stack; `ExampleSmokeValidation` for README/example construction. This
feature adds a classpath-isolation check and a published-POM assertion

**Target Platform**: JVM library consumed by Gatling simulations via sbt, Gradle, and Maven

**Project Type**: Single-module JVM library (Scala core plus a Java facade), published to Sonatype /
Maven Central

**Performance Goals**: N/A — this feature changes packaging, not runtime behavior. The existing suites
are the regression guard that runtime behavior is unchanged

**Constraints**: Published POM must declare only Maven Central-resolvable coordinates in inherited
scopes; the default DSL entry point must neither reference nor initialise a Confluent type; every
published Scala and Java signature must keep compiling for at least this minor release; the build must
compile and test against the same client version consumers resolve

**Scale/Scope**: 4 source files carry the Confluent coupling (`request/KafkaSerdesImplicits.scala`,
`javaapi/checks/KafkaChecks.scala`, `javaapi/KafkaDsl.java`, `javaapi/expressions/Builders.java`), plus
`project/Dependencies.scala`, `build.sbt`, `README.md`, and one example simulation. Two tracked issues
([#185](https://github.com/galax-io/gatling-kafka-plugin/issues/185),
[#214](https://github.com/galax-io/gatling-kafka-plugin/issues/214)) plus one companion issue this
plan recommends opening

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0. Answer each gate; any NO must be justified in
Complexity Tracking below or the plan does not proceed.*

- [x] **I. Published API Compatibility**: **YES, this feature changes public API surface** — and the
      obligations are met as follows. No Scala or Java *signature* changes: the declared types of the
      affected members are already Confluent-free, so every published entry point keeps compiling.
      Four members gain `@deprecated` annotations naming their replacement and their `1.3.0` removal
      release, satisfying the one-minor deprecation window. A README Migration Guide entry is planned
      (FR-011) covering the dependencies consumers must now declare and the one-line import change for
      Schema Registry Avro users. The README compatibility table gains a Kafka client column (FR-013).
      `ExampleSmokeValidation` stays green and its examples move to the documented opt-in import, so it
      validates the consumer path rather than an internal shortcut. **One deviation is recorded in
      Complexity Tracking**: consumers who had worked around the defect by configuring the Confluent
      resolver themselves will need to declare two dependencies on upgrade. That is a build-level
      break with no source-level counterpart, and it is why the migration entry is mandatory rather
      than advisory.
- [x] **II. Real Broker Over Mocks**: The feature changes the Kafka client version, which touches every
      Kafka interaction, so the existing Testcontainers and Compose-stack suites are the regression
      guard and must pass unchanged — no suite is relaxed, sampled, or disabled to accommodate the
      change. The new classpath-isolation check (R7) runs real produce and request-reply simulations
      against Testcontainers with the Confluent artifacts absent; the classpath is what varies, not the
      broker. No mock is introduced anywhere.
- [x] **III. Layer Separation & Single Wire Contract**: Untouched. `KafkaSender`,
      `KafkaMessageTracker`, `KafkaMessageTrackerPool`, and `DynamicKafkaConsumer` are not modified.
      `KafkaProtocolMessage` and `KafkaMatcher` are not extended or duplicated. The one new construct
      is an opt-in entry point object that relocates existing serde members — it introduces no
      abstraction, and it has a real caller (the moved examples and Java tests) from the first commit,
      satisfying the no-speculative-abstraction rule.
- [x] **IV. Test-First for Behavior Change**: Two failing-first tests, both of which fail against
      today's code for the exact reason this feature exists. The published-POM assertion fails naming
      all four unresolvable coordinates. The classpath-isolation check fails at `Predef` initialisation
      with `NoClassDefFoundError: GenericAvroSerde`. Both are written before the corresponding change
      and are not deferred to a later phase.
- [x] **V. One Concern per Change, Always Green**: Spec artifacts commit first as
      `docs(speckit): add 005-classpath-dependency-shedding spec/plan/tasks`, before any `fix`/`feat`.
      Work decomposes to one semantic commit per issue — see Issue Decomposition below — each green on
      its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`. The README and migration
      documentation ride with the issue commit that makes them true, since a documented installation
      path that does not yet work would be a false statement rather than a separable concern. Every PR
      carries milestone 10 and a `Closes #NNN`.
- [x] **Constraints**: No new dependency is added. The Kafka client relocation is a coordinate change
      within the same upstream line, not a version upgrade, and it is the subject of the approved
      FR-018 decision. This feature **restores** compliance with the standing constraint that "Avro4s
      and Confluent Schema Registry support is `provided` scope and MUST remain optional: the plugin
      MUST stay usable with plain serialization and no Schema Registry on the classpath" — which the
      current build violates. No supported Gatling version changes.

### Post-Phase 1 re-check

Re-evaluated against the Phase 1 design in [contracts/](./contracts/) and
[data-model.md](./data-model.md): **all gates still pass**, with two verification gates carried into
implementation and one deviation recorded.

- Gate I is the one that tightened. The design's ability to keep every signature in place rests on a
  property verified for Scala (declared types are Confluent-free) but **not yet verified for Java**:
  whether javac and kotlinc can compile a call to `KafkaDsl.avro(JExpression, Serializer, Deserializer)`
  while two sibling overloads name a `SchemaRegistryClient` that is absent from the classpath. If that
  fails, those two overloads cannot stay in place and Gate I is no longer satisfiable without either a
  Java-source break or leaving Schema Registry inherited for Java consumers. This is a decision gate,
  not an assumption — see Verification Gates.
- Gate II tightened similarly: R2 showed the build resolves to the Confluent client despite declaring
  the Apache one, so "the existing suites pass" is meaningless until the version pin lands. The pin is
  ordered before the suites are treated as evidence.

## Verification Gates

Two findings are carried into implementation as gates that must be closed with evidence, not assumed.
Neither is an open design question; both are claims that a spike could not settle.

| Gate | Claim to establish | How | If it fails |
| --- | --- | --- | --- |
| **G1 — client pin** | The build compiles and tests against the same Kafka client version consumers resolve | Pin the client version, re-run `sbt "clean; compile; Test/compile; evicted"`, confirm no `kafka-clients` conflict is reported | Investigate whether the opt-in artifacts can be excluded from the plugin's own compile classpath; escalate before proceeding, since an unpinned build makes every downstream test result unattributable |
| **G2 — Java overload resolution** | A Java or Kotlin consumer can compile against the facade with no Schema Registry artifact present | Compile a plain Java and a plain Kotlin simulation against the published artifact with Confluent absent, exercising the non-Schema-Registry `avro` overload | The `SchemaRegistryClient`-typed overloads must move to the opt-in Java class, which is a Java-source break. Raise with the maintainer before implementing — it may change the release's version number |

## Issue Decomposition

Constitution Principle V requires one tracked issue per semantic commit. Milestone 10 currently holds
two issues, and the verified scope is wider than either records.

| Commit | Issue | Scope |
| --- | --- | --- |
| `docs(speckit): add 005-classpath-dependency-shedding spec/plan/tasks` | — | Spec artifacts, landing first |
| `fix(build): resolve every inherited dependency from Maven Central (#NEW)` | **companion issue to open** | Relocate the two `org.apache.kafka` artifacts to Apache coordinates; pin the client version (G1); POM assertion test |
| `fix(build): make Confluent Avro support opt-in (#185)` | [#185](https://github.com/galax-io/gatling-kafka-plugin/issues/185) | Move the two `io.confluent` artifacts out of the inherited set; defer the eager initialisers; add the opt-in entry point; classpath-isolation test; README installation + migration entries |
| `refactor(request): deprecate the unused Kafka Streams surface (#214)` | [#214](https://github.com/galax-io/gatling-kafka-plugin/issues/214) | Deprecate both implicits; remove the dead `avroCompiler` declaration and the unused example imports |

**Action required before implementation**: open the companion issue for the Kafka client relocation and
assign it to milestone 10, or extend #185's scope to cover it. #185 as written names only the two
`io.confluent` artifacts; the fix must cover four, and Principle V does not permit smuggling
unattributed scope into a commit that cites #185.

## Project Structure

### Documentation (this feature)

```text
specs/005-classpath-dependency-shedding/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
│   ├── published-pom.md         # What the published artifact may declare
│   └── dsl-entry-points.md      # What moves, what stays, what is deprecated
├── checklists/
│   └── requirements.md  # Spec quality checklist (/speckit-specify output)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
project/
└── Dependencies.scala                    # coordinate relocation, scope changes, dead declaration removal
build.sbt                                 # dependency wiring, version pin (G1), POM assertion hook

src/main/scala/org/galaxio/gatling/kafka/
├── request/KafkaSerdesImplicits.scala    # defer eager Avro val; deprecate Avro + Streams members
├── confluent.scala                       # NEW — opt-in Schema Registry Avro entry point
├── avro4s.scala                          # unchanged — the precedent this mirrors
├── KafkaDsl.scala                        # unchanged — mixes the (now Confluent-free) serdes trait
└── Predef.scala                          # unchanged — must stop touching Confluent transitively

src/main/java/org/galaxio/gatling/kafka/javaapi/
├── checks/KafkaChecks.scala              # defer eager Avro val
├── KafkaDsl.java                         # deprecate Schema Registry overloads (pending G2)
└── expressions/Builders.java             # deprecate Schema Registry constructor (pending G2)

src/test/scala/org/galaxio/gatling/kafka/
├── build/PublishedPomSpec.scala          # NEW — inherited scopes carry only Central coordinates
├── classpath/                            # NEW — plain simulations with Confluent absent
└── examples/BasicSimulation.scala        # remove unused Kafka Streams imports

README.md                                 # Installation (opt-in coordinates + resolver),
                                          # Compatibility (Kafka client column), Migration Guide
.github/workflows/ci.yml                  # scratch-project resolution job for sbt, Gradle, Maven
```

**Structure Decision**: Single module, unchanged. This feature adds one main source file
(`confluent.scala`, deliberately a sibling of the existing `avro4s.scala` so the opt-in idiom is
visibly one pattern rather than two) and two test areas. Everything else is an edit to a file that
already exists. No new module, source root, or build project is introduced — the defect is in what the
existing module declares, not in how it is organised.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| **Principle I** — consumers who had configured the Confluent resolver themselves must declare two dependencies on upgrade, a build-level break shipped in a minor release rather than a major | The two `io.confluent` artifacts have no Maven Central equivalent at any version (verified, R1), so there is no way to satisfy FR-001 while keeping them inherited. Keeping them inherited for a full deprecation cycle would leave the S1 defect shipping for another release; the plugin is currently unresolvable for any consumer following the documented installation path | *Deprecate for one minor, remove in 2.0* — rejected because the artifacts are the defect; retaining them retains the defect, and nothing is deprecated in the source (no signature changes), so there is no entry point to annotate. *Ship it as 2.0.0* — rejected by the maintainer at specification time as promoting a packaging fix to a major bump. The break is confined to build files, has a mechanical fix stated in the migration guide, and affects only consumers who had already worked around the defect with an undocumented resolver configuration |
| **Principle V** — the README and migration documentation ride inside the issue commits rather than a separate docs PR | The documentation states the installation path. Landing "add these coordinates" before the code that requires them, or after it, means the repository asserts something untrue at one of the two commits | *Separate docs PR* — rejected because it guarantees an interval in which either the documented install is wrong or the shipped artifact is undocumented. The constitution's rule targets opportunistic docs bundled into feature work; documentation that is part of the contract being changed is the same concern, not a second one |
