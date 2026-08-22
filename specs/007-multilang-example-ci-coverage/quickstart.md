# Quickstart: Multi-Language Example Coverage in CI

**Feature**: `007-multilang-example-ci-coverage` | **Date**: 2026-08-21

How to run and verify this feature locally, and the deliberate-break drills FR-007a requires.

Nothing here is bespoke. Each language runs through the command its own build system provides,
because that is the command a user of this plugin runs.

---

## Prerequisites

- JDK 17 (Temurin), sbt, Maven 3.9.10+, Docker
- The local broker stack:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Wait for `gatling-kafka-topic-init` to exit 0 — it creates the topics, including the thirteen
`ex.<lang>.<example>.t` topics, one per example so that no two share a topic.

No Kotlin compiler and no Gradle install are needed: `examples/kotlin` carries a committed wrapper.

---

## 1. The coverage contract — no broker needed

The fastest check, and the one that must stay usable with nothing running.

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"
```

**Expected**: the thirteen examples listed by language, and `13 topic(s) checked`. It fails if an
example on disk has no recorded coverage ([C3](./contracts/example-coverage.md)), if two examples
share a topic, if a topic is missing from either broker definition, or if an example names a topic in
a way the reader cannot resolve ([C6](./contracts/example-coverage.md)).

**Prove it needs nothing running** — stop the stack and run it again. Still green.

---

## 2. The plugin's own simulations

```bash
sbt "Gatling / test"
```

**Expected**: exactly three — `KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`,
`KafkaConcurrencyLoadTest`. These are test harnesses, not examples. Each runs in its own forked JVM
(`Gatling / testGrouping`), so a leak in the load test cannot reach the others.

---

## 3. The examples — one project per language

Publish the plugin once, then run whichever project you like. Each depends on the published artifact
exactly as a user's project does.

```bash
sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2

(cd examples/scala  && sbt "Gatling / test")        # 5 simulations
mvn -f examples/java/pom.xml verify                 # 4 simulations
(cd examples/kotlin && ./gradlew gatlingRun --all)   # 4 simulations
```

Each asserts its own request count and a 100% success rate
([C1](./contracts/example-coverage.md), [C5](./contracts/example-coverage.md)).

To run them against a released plugin instead of the local snapshot, change the version in that
project's build file — or, for Maven, `mvn verify -Dgatling-kafka-plugin.version=1.2.0`.

**Do not** expect `Gatling / testOnly` in the plugin build to run a Java or Kotlin simulation. It
selects nothing and exits 0 — see [R1](./research.md).

`examples/kotlin` pins Gradle 8.12 in its wrapper: `io.gatling.gradle` 3.13.5.4 is the release
matching Gatling 3.13.5, and it cannot configure on Gradle 9. Move the wrapper and the plugin
together or not at all.

---

## 4. The full local gate

What CI does, in order:

```bash
sbt scalafmtCheckAll scalafmtSbtCheck
sbt clean compile
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"
sbt coverage "Gatling / test" test coverageOff coverageReport
sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2
(cd examples/scala  && sbt "Gatling / test")
mvn -B -f examples/java/pom.xml verify
(cd examples/kotlin && ./gradlew gatlingRun --all --console=plain)
```

---

## 5. Deliberate-break drills — required for acceptance

One per language, run by hand, recorded in the PR. FR-007b requires each defect to be one the
language's coverage actually claims to catch — a defect that would have failed anyway proves nothing.

### Drill 1 — Scala, run-time defect

Compiles cleanly. Point a reply topic at a topic nothing produces to:

```text
.replyTopic("ex.scala.match.t")   →   .replyTopic("ex.scala.nonexistent.t")
```

```bash
(cd examples/scala && sbt "Gatling / test")
```

**Expected**: red on the success-rate assertion. Revert and confirm green.

### Drill 2 — Java, run-time defect

Compiles cleanly. Remove one of `ProducerSimulation`'s three sends, leaving the scenario intact:

```bash
mvn -f examples/java/pom.xml verify
```

**Expected**: red on the request-count assertion — `count of all events is 3.0 : false (actual : 2.0)`.
Revert and confirm green.

### Drill 3 — Kotlin, compile-time defect

```text
val scn = scenario("Basic")   →   val scn = scenario(   // unbalanced
```

```bash
(cd examples/kotlin && ./gradlew gatlingRun --all)
```

**Expected**: `compileGatlingKotlin` fails, naming the file. Revert and confirm green.

### Drill 4 — a topic clash is caught before anything runs (C6)

Point one example's topic at another's, or delete a topic from `docker-compose.kafka.yml`:

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"
```

**Expected**: red, naming the topic and the examples that share it, or the broker definition that
omits it. Revert and confirm green.

### Drill 5 — an uncovered example is detected (C3)

```bash
cp examples/java/src/test/java/org/galaxio/examples/javaapi/MatchSimulation.java \
   examples/java/src/test/java/org/galaxio/examples/javaapi/StraySimulation.java
# fix the class name inside, then:
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"
```

**Expected**: red — the new example has no coverage level. Delete the file and confirm green.

---

## Acceptance checklist

- [ ] Coverage contract green with the whole stack stopped; 13 examples, 13 topics (C3, C6)
- [ ] `sbt "Gatling / test"` in the plugin — exactly the 3 test harnesses, one JVM each
- [ ] `examples/scala` green — 5 simulations (C1, C5)
- [ ] `examples/java` green — 4 simulations (C1, C5)
- [ ] `examples/kotlin` green — 4 simulations (C1, C2, C5)
- [ ] Drills 1–3 recorded, one per language (FR-007a, SC-004)
- [ ] Drills 4 and 5 recorded (C6, C3)
- [ ] `README.md`, `AGENTS.md`, constitution re-read against reality (C7, SC-006)
