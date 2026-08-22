# Data Model: Multi-Language Example Coverage in CI

**Feature**: `007-multilang-example-ci-coverage` | **Date**: 2026-08-19

The "data" of this feature is the coverage inventory: which examples exist, what CI does with each,
and what must be true for each to run. It is a real model, not a metaphor — FR-005 requires the
covered set to be derived from the example sources rather than hand-maintained, so this table is the
specification of what that derivation must produce.

---

## Entity: Example Simulation

An example simulation published as user-facing documentation.

| Field | Type | Rule |
|---|---|---|
| `language` | `Scala` \| `Java` \| `Kotlin` | Derived from the source root the file sits under. |
| `fqcn` | fully-qualified class name | Derived from package path + file base name. |
| `sourcePath` | path | Under one of the three example directories. |
| `coverageLevel` | see below | Exactly one per example. Never absent. |
| `topics` | set of topic names | Disjoint from every other covered example's set (DR-2). |
| `assertions` | request count + success rate | Required when `coverageLevel = Executed` (DR-3). |
| `correction` | text \| none | Required when the example does not run as written (DR-4). |

**Identity**: `fqcn` is unique across the inventory. Each language has its own project and its own
package — `org.galaxio.examples.{scalaapi,javaapi,kotlinapi}` — so identical simple names cannot
collide. An earlier revision put Java and Kotlin in one Maven module, where they compiled into a
shared `test-classes` and the distinct packages were the only thing preventing one from overwriting
the other; separate projects make that impossible rather than merely avoided.

**Excluded from the inventory** (in the examples package but not examples): `KafkaGatlingTest`,
`KafkaJavaapiMethodsGatlingTest`, `KafkaConcurrencyLoadTest`, `GatlingRunner`,
`ReadmeExamplesCompileOnly`, `ExampleSmokeValidation`, `ExampleInventory`. The derivation must
distinguish these; see DR-1.

---

## Entity: Coverage Level

| Value | Meaning | Applies to |
|---|---|---|
| `Executed` | Run against a real broker on every CI run, with assertions | all thirteen examples |
| `CompileOnly(reason)` | Compiled, deliberately not run, with the reason recorded | any example that cannot be made to run |

Where an example runs depends on its language, not on its coverage level: Scala under
`sbt Gatling/test`, Java under `mvn verify` in `examples/java`, Kotlin under `./gradlew gatlingRun --all` in
`examples/kotlin`. sbt cannot run the latter two at all (R1).

**State transitions**: an example enters at `CompileOnly` only by exception. Every example in this
feature's inventory is `Executed`; `CompileOnly` exists so that a future example which genuinely
cannot run is visible rather than silently dropped (FR-004).

---

## The inventory

### Scala examples — target `Executed`, in `examples/scala`

| Example | Topics (after R4) | Profile | Assertions | Correction required |
|---|---|---|---|---|
| `Avro4sSimulation` | `ex.scala.avro4s.t` | 1 user, 2 requests | 2 requests, 100% ok | none — topic + assertions only |
| `AvroClassWithRequestReplySimulation` | `ex.scala.avrorr.t` (echo) | 1 user, 1 request | 1 request, 100% ok | **yes** — `"schRegUrl"` → `http://localhost:9094`; empty `case class MyAvroClass()` → real Avro type |
| `BasicSimulation` | `ex.scala.basic.t` (echo) | small, ≥1 user | requests = profile, 100% ok | **yes** — 2nd exchange sends `myTopic2` / replies `test.t1` with nothing echoing between them, and checks `$.M is DKF` against `{"m":"dkf"}`; `atOnceUsers(50)` reduced |
| `MatchSimulation` | `ex.scala.match.t` (echo) | 1 user (bound by the constant matcher) | 1 request, 100% ok | none — topic + assertions only |
| `ProducerSimulation` | `ex.scala.producer.t` | 5 users, 2 requests each | 10 requests, 100% ok | none — topic + assertions only |

### Java examples — target `Executed`, in `examples/java`

| Example | Topics (after R4) | Profile | Assertions | Correction required |
|---|---|---|---|---|
| `AvroClassWithRequestReplySimulation` | `ex.java.avrorr.t` (echo) | 1 user, 1 request | 1 request, 100% ok | **yes** — `CachedSchemaRegistryClient("schRegUrl")` → `http://localhost:9094`; empty `private static class MyAvroClass` → real Avro type |
| `BasicSimulation` | `ex.java.basic.t` (echo) | 5 users, 1 request each | 5 requests, 100% ok | none — topic + assertions only |
| `MatchSimulation` | `ex.java.match.t` (echo) | 1 user (bound by the constant matcher) | 1 request, 100% ok | none — topic + assertions only |
| `ProducerSimulation` | `ex.java.producer.t` | 1 user, 3 requests | 3 requests, 100% ok | **yes** — no `setUp(...)` at all; nothing to execute |

### Kotlin examples — target `Executed`, in `examples/kotlin`

| Example | Topics | Profile | Assertions | Correction required |
|---|---|---|---|---|
| `AvroClassWithRequestReplySimulation.kt` | `ex.kotlin.avrorr.t` (echo) | 1 user, 1 request | 1 request, 100% ok | **yes** — rewritten |
| `BasicSimulation.kt` | `ex.kotlin.basic.t` (echo) | 5 users, 1 request each | 5 requests, 100% ok | **yes** — rewritten |
| `MatchSimulation.kt` | `ex.kotlin.match.t` (echo) | 1 user | 1 request, 100% ok | **yes** — rewritten |
| `ProducerSimulation.kt` | `ex.kotlin.producer.t` | 1 user, 3 requests | 3 requests, 100% ok | **yes** — rewritten |

**All four had to be rewritten: none of them compiled.** Nothing had compiled them since `#181`.
`ProducerSimulation.kt` had an unclosed `.exec(` and used `Session` as a type with no import; it and
`AvroClassWithRequestReplySimulation.kt` used `KafkaAvroSerializer`, `CachedSchemaRegistryClient`,
`Serializer` and `Deserializer` with no imports at all, against a `MyAvroClass` that exists only as a
nested class inside the *Scala* example. `BasicSimulation.kt` still built its protocol with the
`.topic()` removed in 1.0.0 — the defect that motivated issue #240. Only `MatchSimulation.kt` was
close to clean.

---

## Entity: CI Broker Topic Inventory

Two independent definitions that must agree:

| Definition | Location | Mechanism |
|---|---|---|
| CI | `.github/workflows/ci.yml` | `KAFKA_CREATE_TOPICS` on the `kafka` service |
| Local | `docker-compose.kafka.yml` | the `topic-init` service's `kafka-topics --create` chain |

**Existing topics** (unchanged by this feature): `myTopic1`, `test.t1`, `myTopic2`, `test.t2`,
`myTopic3`, `test.t3`, `myTopic4`, `myTopic5`, `test.t5`, `myTopic6`, `test.t6`, `load.request`,
`load.reply`. The Compose file additionally creates `test.t`, which CI does not.

**Topics this feature adds** — thirteen, one per example, added to **both** definitions:

```text
ex.scala.avro4s.t   ex.scala.avrorr.t   ex.scala.basic.t   ex.scala.match.t   ex.scala.producer.t
ex.java.avrorr.t    ex.java.basic.t     ex.java.match.t    ex.java.producer.t
ex.kotlin.avrorr.t  ex.kotlin.basic.t   ex.kotlin.match.t  ex.kotlin.producer.t
```

Each is 1 partition, replication factor 1, matching every existing topic in both files.

**Constraint**: the two definitions are maintained separately by design — both files already carry a
comment saying so. This feature adds the same thirteen topics to both and does not attempt to unify
them.

---

## Entity: Compatibility Gate

| Field | Before | After |
|---|---|---|
| Example set | hard-coded list of 9 FQCNs | derived from the example source tree (DR-1) |
| Kotlin examples | absent | present in the inventory, so one added without coverage fails here |
| Per-example check | `Class.forName` + `getDeclaredConstructor()` | `Class.forName` + `getDeclaredConstructor().newInstance()` |
| What a failure proves | the class is missing or has no no-arg constructor | the scenario or protocol no longer builds |
| External services | none required | none required (FR-011, unchanged) |

---

## Entity: Deliberate-Break Drill

An acceptance artifact, not a standing check (clarification Q4).

| Field | Scala | Java | Kotlin |
|---|---|---|---|
| Where it runs | `sbt Gatling/test` | `mvn verify` | `mvn verify` |
| Defect class required (FR-007b) | run-time only, compiles cleanly | run-time only, compiles cleanly | compile-time |
| Example broken | recorded at acceptance | recorded at acceptance | recorded at acceptance |
| CI job that went red | recorded at acceptance | recorded at acceptance | recorded at acceptance |

A defect that would have failed anyway proves nothing (FR-007b) — so the Scala and Java drills must
not use a compile error, and the Kotlin drill must not use something the compiler would accept.

---

## Derivation rules

- **DR-1 — What counts as an example.** A source file under one of the three example directories
  whose class extends `io.gatling.core.scenario.Simulation` (Scala) or
  `io.gatling.javaapi.core.Simulation` (Java/Kotlin), **minus** the explicit exclusion list of test
  harnesses and utilities named above. The exclusion list is the one hand-maintained thing left, and
  it is small, closed, and fails loudly when wrong: an unexcluded harness would be run as an example
  and its absence from the inventory would fail C3.
- **DR-2 — Topic disjointness.** No two covered examples may share a topic. Violating this
  reintroduces exactly the cross-attribution the plugin exists to prevent, and `MatchSimulation`'s
  constant matcher would accept the stray record.
- **DR-3 — Assertions bound by the profile.** An example's assertions are written to what its
  injection profile and matching strategy actually guarantee, never above it (FR-006b). Raising a
  profile without raising its assertion, or the reverse, must fail rather than pass.
- **DR-4 — A correction is bounded.** A correction under FR-002a may change topics, injection
  volume, service endpoints, payload types, and may add `setUp` and assertions. It may **not**
  change which DSL calls the example demonstrates, or their order (FR-002b).
- **DR-5 — One coverage level per example, always present.** An example with no coverage level is a
  CI failure, not a default. This is what makes "someone added an example and nobody wired it up"
  detectable (FR-005, SC-007).
