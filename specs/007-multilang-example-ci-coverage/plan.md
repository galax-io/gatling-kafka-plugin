# Implementation Plan: Multi-Language Example Coverage in CI

**Branch**: `007-multilang-example-ci-coverage` | **Date**: 2026-08-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/007-multilang-example-ci-coverage/spec.md`

## Summary

No simulation the project publishes as a documentation example is run by CI, in any of the three
languages it documents. This feature makes all thirteen run against a real broker, strengthens the
compatibility gate so it verifies what the project claims it verifies, and corrects every statement
that overstates it.

Three findings shaped the approach, each overturning something the originating issue assumed:

1. **`Gatling / testOnly` cannot run a Java or Kotlin simulation, and no configuration changes that.**
   `io.gatling.javaapi.core.Simulation` does not extend `io.gatling.core.scenario.Simulation`, and
   `gatling-test-framework` declares exactly one sbt fingerprint, matching only the Scala superclass.
   Naming one selects nothing and exits 0 — the failure mode a coverage feature can least afford.
   This is a product boundary: Gatling's sbt plugin supports Scala only. See [R1](./research.md).
2. **The Scala examples are not run either.** The three simulations CI runs are test harnesses
   sharing the `examples` package, not the examples the README links. See [R2](./research.md).
3. **Eight of the nine JVM examples were broken**, and all four Kotlin ones failed to compile. Only
   `Avro4sSimulation` worked. See [R3](./research.md).

Each language therefore runs through its own build system's Gatling task — nothing bespoke.
`sbt Gatling/test` covers the Scala examples together with the test harnesses; the Java and Kotlin
examples live in `examples/java` and `examples/kotlin`, and the Scala ones in `examples/scala` —
three consumer projects depending on the published artifact, each run by its own build tool's Gatling
task. Those are the commands their users run, and they additionally exercise the consumer coordinates
`README.md` documents.

## Technical Context

**Language/Version**: Scala 2.13.18, Java 17, Kotlin 2.4.10 (in the Maven consumer project)

**Primary Dependencies**: Gatling 3.13.5 (`gatling-core`, `gatling-core-java` provided;
`gatling-test-framework`, `gatling-charts-highcharts` in `it,test`), Apache Kafka clients 3.9.2,
Confluent Avro serializers 7.9.9 (`provided`), Avro 1.12.1. No new dependency is added to the
published artifact. The consumer project declares gatling-maven-plugin 4.19.1 and kotlin-maven-plugin
2.4.10 for itself.

**Storage**: N/A

**Testing**: sbt; MUnit + Testcontainers for unit/integration; Gatling simulations against the CI
broker stack (Kafka, Zookeeper, Schema Registry) defined in `.github/workflows/ci.yml`

**Target Platform**: JVM 17 on `ubuntu-24.04` in CI; the `docker-compose.kafka.yml` stack locally

**Project Type**: Gatling protocol plugin (library) published to Sonatype

**Performance Goals**: None. No wall-clock budget is committed (clarification Q5); cost is bounded by
keeping every covered example at the smallest injection volume its assertions need.

**Constraints**: The compatibility gate must run with no broker, registry, or network (FR-011).
Kotlin must not enter the sbt build. The plugin's published API must not change. No bespoke run
mechanism: each language uses its build system's own Gatling task.

**Scale/Scope**: 13 published examples (5 Scala, 4 Java, 4 Kotlin); 12 corrected; 1 consumer
project; 1 CI workflow; 2 broker topic inventories; 1 compatibility gate; 3 documents including the
constitution.

## Constitution Check

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility** — No public Scala DSL or `javaapi` signature, protocol
      default, or serialized format changes. Changes to example sources are corrections under
      FR-002a, bounded by FR-002b. The gate is *strengthened*: after this feature
      `ExampleSmokeValidation` really does construct every example, which makes Principle I's own
      claim true for the first time. No `!:` marker, no Migration Guide entry, no deprecation.
- [x] **II. Real Broker Over Mocks** — This feature adds broker-backed coverage and removes none. No
      mock is introduced anywhere; every covered example runs against the real CI broker.
- [x] **III. Layer Separation & Single Wire Contract** — No plugin source under `src/main` is
      touched. `KafkaSender` / `KafkaMessageTracker` / `DynamicKafkaConsumer` are untouched, and no
      new message or matcher type appears.
- [x] **IV. Test-First for Behavior Change** — The deliverable *is* test coverage, so the ordering is
      inverted in the natural way: each covered example is added to the runner and demonstrated to
      go red on a deliberate defect before that defect is reverted (FR-007a). For the gate, the
      strengthening lands with a construction-time break proving it fails first.
- [x] **V. One Concern per Change, Always Green** — Spec artifacts commit first as
      `docs(speckit): add 007-multilang-example-ci-coverage spec/plan/tasks`. Implementation splits
      into single-concern PRs (see Delivery Slices), each green under
      `sbt scalafmtCheckAll scalafmtSbtCheck compile test`, each carrying milestone
      **v1.13.0 Test suite integrity** and closing its issue.
- [x] **Constraints** — No sbt dependency is added; Avro and Schema Registry stay `provided`; no
      supported Gatling version changes. The earlier concern — provisioning a Kotlin compiler in CI —
      is gone: `kotlin-maven-plugin` brings its own, so there is no toolchain to install and nothing
      to approve.

**Post-Phase 1 re-check**: the design is smaller than planned. It adds one sbt setting
(`Gatling / parallelExecution := false`) and one consumer project; it *removes* the `exampleRun`
task, `ExampleSimulationRunner`, `exampleClasspath` and `scripts/check-kotlin-examples.sh`. No plugin
source, no published API, no new sbt dependency.

## Project Structure

### Documentation (this feature)

```text
specs/007-multilang-example-ci-coverage/
├── plan.md              # This file
├── research.md          # Phase 0 output — R1..R8
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── example-coverage.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
build.sbt                                   # - exampleRun, - exampleClasspath, + Gatling/parallelExecution
.github/workflows/ci.yml                    # Gatling/test; publishM2 + mvn verify; + topics
docker-compose.kafka.yml                    # + topics, kept in step with ci.yml
src/test/scala/io/gatling/
└── GatlingInternals.scala                  # trimmed to installTestConfiguration()
src/test/scala/org/galaxio/gatling/kafka/examples/
├── ExampleInventory.scala                  # derives all 13 examples across the two projects
├── ExampleSmokeValidation.scala            # constructs the Scala examples; needs no broker
├── Avro4sSimulation.scala                  # topic + assertions
├── AvroClassWithRequestReplySimulation.scala   # corrected: own type + own serde, real registry URL
├── BasicSimulation.scala                   # corrected: echo topic, correct check, smaller profile
├── MatchSimulation.scala                   # corrected: port, topic, timeout, assertions
└── ProducerSimulation.scala                # corrected: port, topic, assertions
examples/scala/                             # NEW — sbt consumer project
├── build.sbt, project/
└── src/test/scala/org/galaxio/examples/scalaapi/*.scala  # 5, moved and corrected
examples/java/                              # NEW — Maven consumer project
├── pom.xml
└── src/test/java/org/galaxio/examples/javaapi/*.java     # 4, moved and corrected
examples/kotlin/                            # NEW — Gradle consumer project
├── build.gradle.kts, settings.gradle.kts, gradle wrapper pinned to 8.12
└── src/gatling/kotlin/org/galaxio/examples/kotlinapi/*.kt   # 4, moved and rewritten
README.md                                   # what each check proves; example links
AGENTS.md                                   # Test Model corrected
.specify/memory/constitution.md             # Development Workflow corrected; 1.0.1
```

**Structure Decision**: All examples leave the plugin build for a consumer project per language, each
on the build tool that language's users use. That is not a workaround for sbt's inability to run Java
and Kotlin — it mirrors how the plugin is actually consumed, and each project resolves the
*published* artifact, so it checks the contract a user gets rather than this build's internal test
classpath. The plugin's own `src/test/scala` keeps only the three test harnesses.

## Delivery Slices

One concern per PR (Principle V), each independently green and independently valuable.

| Slice | Concern | Delivers | Depends on |
|---|---|---|---|
| **S1** | Compatibility gate | `ExampleSmokeValidation` constructs each example and derives its list from the example sources; a construction break fails it | — |
| **S2** | Example corrections | The six defective examples run as written; topics renamed per-example; assertions added | — |
| **S3** | Native run | one consumer project per language under `examples/`, each run by its own build tool; CI and Compose topic inventories updated | S2 |
| **S4** | Kotlin | folded into S3 — `kotlin-maven-plugin` in the consumer project compiles and runs them; no toolchain to provision | — |
| **S5** | Documentation | `README.md`, `AGENTS.md`, constitution 1.0.1 corrected to match what now exists | S1, S3, S4 |

S5 last on purpose: correcting a claim before the thing it describes exists would make the
documentation wrong in the other direction.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| Two more build systems enter the repository (`examples/java` Maven, `examples/kotlin` Gradle) | sbt cannot run a Java or Kotlin simulation at all (R1), and Gatling directs those users to Maven or Gradle. Without it, eight of thirteen examples have no runtime coverage — the gap #240 is about. | *A bespoke sbt task driving `io.gatling.app.Gatling`* — three entities no consumer of this plugin could copy; rejected by the user. *`Test/runMain io.gatling.app.Gatling`* — `main` calls `sys.exit`, so chained invocations exit 0 having run only the first (measured). *Gradle* — `io.gatling.gradle` 3.13.5.4 cannot configure on the installed Gradle 9.4.1; only 3.15.1.2 pinned back to Gatling 3.13.5 works, a pairing Gatling never tests. |
| Twelve published examples are edited | Clarification Q1, and R3: eight were broken at run time and four did not compile. Each defect is one a user hits by copying the example. | *Mark them compile-only* — rejected in Q1. *Delete them* — leaves produce-only and Avro request-reply with no example at all. |

## Phase 0 Output

[research.md](./research.md) — R1 (fingerprint blocker and the `Gatling.fromArgs` route), R2 (no
example runs in any language), R3 (the six defective examples and each correction), R4 (per-example
topic isolation), R5 (fork and JVM options), R6 (the Kotlin check), R7 (strengthening the gate), R8
(documentation and constitution corrections). No `NEEDS CLARIFICATION` remains.

## Phase 1 Output

- [data-model.md](./data-model.md) — the coverage inventory: every example, its language, coverage
  level, topics, assertions, and required correction.
- [contracts/example-coverage.md](./contracts/example-coverage.md) — the contracts CI must satisfy
  (C1..C7), each stated so it can fail.
- [quickstart.md](./quickstart.md) — how to run and verify the whole thing locally, including the
  three deliberate-break drills FR-007a requires.
