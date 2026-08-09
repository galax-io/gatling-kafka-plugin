# Quickstart: Validating Classpath and Dependency Shedding

**Feature**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09

How to prove this feature works, in the order the evidence should be gathered. Every step states what a
pass looks like and what the current (pre-fix) result is, so a step that passes before the change is a
broken test rather than good news.

**Contracts referenced**: [published-pom.md](./contracts/published-pom.md) (C1–C5),
[dsl-entry-points.md](./contracts/dsl-entry-points.md) (E1–E5).

---

## Prerequisites

- JDK 17+ and sbt 1.12.15 (the repo pins this in `project/build.properties`)
- Docker, for Testcontainers and the Compose stack
- Gradle and Maven, for the cross-build-tool checks in Step 4
- Network access to Maven Central; **deliberately no Confluent resolver** in the scratch projects

```bash
docker compose -f docker-compose.kafka.yml up -d
```

---

## Step 0 — Baseline: confirm the defect still reproduces

Do this before changing anything. If it does not reproduce, the premise has changed and the plan needs
revisiting.

```bash
curl -s https://repo1.maven.org/maven2/org/galaxio/gatling-kafka-plugin_2.13/1.2.0/gatling-kafka-plugin_2.13-1.2.0.pom
```

**Expected**: four `<dependency>` entries with no `<scope>` (i.e. inherited) whose coordinates are
`org.apache.kafka:kafka-clients:7.9.5-ce`, `org.apache.kafka:kafka-streams-scala_2.13:7.9.5-ce`,
`io.confluent:kafka-streams-avro-serde:7.9.8`, `io.confluent:kafka-avro-serializer:7.9.8`; and no
`<repositories>` element anywhere in the file.

Then read the versions the *next* release would publish, which differ — dependency automation advances
them regularly:

```bash
grep -E "val kafka|val kafkaAvroSerde" project/Dependencies.scala
```

**Expected at `main` = `4516572`**: `7.9.9-ce` and `7.9.9`. Confirm the currently declared vendor
coordinate is not on Central, substituting whatever version the previous command printed:

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/7.9.9-ce/kafka-clients-7.9.9-ce.pom
```

**Expected**: `404`. If a bump has landed since, re-run with the new version — the answer has been
`404` for every `-ce` and `-ccs` version checked, and a `200` here would be a genuinely new finding
worth stopping for.

---

## Step 1 — Gate G1: the build resolves the client it ships

Run after the coordinate relocation and the version pin, before trusting any test result.

```bash
sbt -batch "clean; compile; Test/compile; evicted"
```

**Pass**: exit 0, and the `evicted` output reports **no** version conflict for
`org.apache.kafka:kafka-clients`.

**Current result**: exit 0 but `evicted` reports
`org.apache.kafka:kafka-clients:7.9.9-ccs is selected over {3.9.2}` — the build compiles against the
Confluent client while declaring the Apache one. Until this line is gone, every later step is testing
an artifact that differs from the one consumers get, and Contract C4 is unmet.

---

## Step 2 — Contract C1–C3, C5: the published metadata

```bash
sbt "testOnly *PublishedPomSpec"
```

**Pass**: the inherited dependency set contains no `io.confluent` coordinate and no `org.apache.kafka`
version bearing a `-ce` or `-ccs` suffix; every inherited dependency is justified; at most one is
justified by a deprecation; publication identity is unchanged.

**Current result**: fails, naming all four offending coordinates. This is the failing-first test for
Constitution Principle IV — run it before the build change and keep the output.

Inspect the generated POM directly when diagnosing:

```bash
sbt makePom
```

---

## Step 3 — Contract E1: the default import works without Confluent

The core acceptance check, and the one that distinguishes a real fix from a metadata-only one.

```bash
sbt "testOnly *ClasspathIsolation*"
```

**Pass**: with the `io.confluent` artifacts absent from the runtime classpath, a plain produce
simulation and a plain request-reply simulation with checks both run to completion against a real
broker, and `Predef` initialises without error.

**Current result**: fails at `Predef` initialisation with
`NoClassDefFoundError: io/confluent/kafka/streams/serdes/avro/GenericAvroSerde`.

**Watch for a false pass**: if the harness leaves the Confluent artifacts on the classpath the test
passes vacuously. Assert the absence — the check should fail loudly if `GenericAvroSerde` *is*
loadable, so a misconfigured harness reports itself instead of reporting success.

---

## Step 4 — FR-003: a real consumer, on all three build tools

Contracts C1–C3 test the metadata; this tests what a build tool actually does with it, which is where
scope-handling differences between the tools surface.

```bash
sbt publishM2
```

Then, for each of sbt, Gradle, and Maven, build a scratch consumer project configured with **Maven
Central and the local repository only — no Confluent resolver** — declaring nothing but the plugin, and
containing the minimal produce simulation from the README.

**Pass**: resolution succeeds, the simulation compiles, and it runs against the broker. For each tool,
all three.

**Current result**: resolution fails with unresolved dependencies for all three.

**Why the local repository is allowed**: the version under test is not on Central. Its *transitive*
dependencies still must come from Central alone, which is the property under test. Adding the Confluent
resolver to any scratch project invalidates the check.

---

## Step 5 — Contract E2 and FR-009: the opt-in Avro path

Repeat Step 4's sbt project, this time adding exactly what the README's Avro section says — the two
`io.confluent` coordinates and the Confluent repository — and the opt-in import.

**Pass**: an Avro produce simulation and an Avro body check against a Schema-Registry-backed record both
compile and run. No coordinate or repository outside the README was needed.

**This step is also a documentation test.** Follow the README literally, without consulting the build
definition. If a step requires knowledge not in the README, FR-008 is unmet even though the code works.

---

## Step 6 — Gate G2: the Java and Kotlin facade

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

Then compile a plain Java simulation and a plain Kotlin simulation against the published artifact with
Confluent absent, exercising `avro(JExpression, Serializer, Deserializer)` so overload resolution is
actually forced.

**Pass**: both compile. Per Contract E4, this establishes that the `SchemaRegistryClient`-typed
overloads can stay in place, deprecated.

**If it fails**: stop. Those overloads must move, which is a Java-source break with no deprecation
window and a change to the release's compatibility story. Raise it with the maintainer rather than
deciding it during implementation.

---

## Step 7 — Contract E5: nothing else moved

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport
```

**Pass**: green, with no suite relaxed, skipped, retried, or disabled to accommodate the dependency
change. A suite modified to keep passing is a finding, not a pass — this run is the only evidence that
relocating the Kafka client changed no runtime behavior.

---

## Step 8 — SC-006: an existing consumer upgrades cleanly

Take a plain-serialization simulation written against 1.2.x, change only the plugin version, and build.

**Pass**: compiles and runs with zero changes to build files or sources.

Then take a Schema Registry Avro simulation written against 1.2.x with a self-configured Confluent
resolver, and apply exactly the steps in the Migration Guide.

**Pass**: compiles and runs. If any change beyond the documented steps is needed, FR-011 is unmet.

---

## Evidence checklist

| Step | Contract / requirement | Must fail before the fix |
| --- | --- | --- |
| 0 | Defect reproduces | — (baseline) |
| 1 | C4, gate G1 | yes — conflict reported |
| 2 | C1, C2, C3, C5 | yes — 4 violations |
| 3 | E1, FR-004, FR-005 | yes — `NoClassDefFoundError` |
| 4 | FR-003, SC-001, SC-003 | yes — unresolved dependencies |
| 5 | E2, FR-008, FR-009, SC-004 | n/a — path does not exist yet |
| 6 | E4, gate G2 | unknown — this is what the gate settles |
| 7 | E5, SC-009 | no — must pass before and after |
| 8 | SC-005, SC-006, FR-011 | n/a — needs the release to exist |
