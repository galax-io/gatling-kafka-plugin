# Phase 0 Research: Classpath and Dependency Shedding

**Feature**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09
**Spec**: [spec.md](./spec.md)

All findings below were established by probing live artifact repositories and by a compile spike in this
worktree, not by reading the build definition. Commands and outcomes are recorded so they can be
re-run.

---

## R1 — Which coordinates can replace the Confluent-only Kafka artifacts

**Decision**: Relocate the Kafka client and Kafka Streams Scala artifacts to the Apache-released
coordinates on Maven Central, targeting **3.9.2** — the newest 3.9.x on Central (3.9.3 does not exist).
Keep both inherited by consumers.

**Rationale**:

- Confluent Platform 7.9.x is built from Apache Kafka 3.9.x, so 3.9.x is the equivalent line rather
  than an upgrade. Version currency is explicitly out of scope for this feature. `main` currently pins
  `7.9.9-ce`, which is still the 7.9 line, so 3.9.2 remains the matching target.
- The broker images this project tests against are `confluentinc/cp-kafka:7.9.5` in both
  `docker-compose.kafka.yml` and the Testcontainers integration specs — i.e. Apache 3.9.x brokers. A
  3.9.x client matches them exactly.
- CI's service broker is `wurstmeister/kafka:2.13-2.8.1` (Apache 2.8.1). Kafka clients are
  backward-compatible across this gap for the APIs in use, and the current 7.9.5-ce client already
  spans it.
- The plugin uses only long-stable core client API. Full inventory of `org.apache.kafka` imports in
  `src/main`: `clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRebalanceListener,
  KafkaConsumer}`, `clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer, Producer,
  RecordMetadata}`, `common.TopicPartition`, `common.errors.WakeupException`, `common.header.*`,
  `common.serialization.*`, plus the two Streams types covered in R4. Nothing version-sensitive.

**Verification performed** (2026-08-09):

| Coordinate | Maven Central | Confluent repo |
| --- | --- | --- |
| `org.apache.kafka:kafka-clients:7.9.2-ccs` | 404 | — |
| `org.apache.kafka:kafka-clients:7.9.5-ce` *(published 1.2.0)* | 404 | 200 |
| `org.apache.kafka:kafka-clients:7.9.5-ccs` | 404 | — |
| `org.apache.kafka:kafka-clients:7.9.8-ce` | 404 | 200 |
| `org.apache.kafka:kafka-clients:7.9.8-ccs` | 404 | 200 |
| `org.apache.kafka:kafka-clients:7.9.9-ce` *(current `main`)* | 404 | 200 |
| `org.apache.kafka:kafka-clients:7.9.9-ccs` | 404 | 200 |
| `org.apache.kafka:kafka-clients:3.9.0` | **200** | — |
| `org.apache.kafka:kafka-clients:3.9.1` | **200** | — |
| `org.apache.kafka:kafka-clients:3.9.2` | **200** | — |
| `org.apache.kafka:kafka-clients:3.9.3` | 404 — does not exist | — |
| `org.apache.kafka:kafka-streams-scala_2.13:3.9.1` | **200** | — |
| `org.apache.kafka:kafka-streams-scala_2.13:3.9.2` | **200** | — |
| `io.confluent:kafka-streams-avro-serde:7.9.8` | 404 | 200 |
| `io.confluent:kafka-avro-serializer:7.9.8` | 404 | 200 |
| `io.confluent:kafka-streams-avro-serde:7.9.9` *(current `main`)* | 404 | 200 |
| `io.confluent:kafka-avro-serializer:7.9.9` *(current `main`)* | 404 | 200 |
| `io.confluent:kafka-schema-registry-client:7.9.8` | 404 | — |

Neither the `-ce` nor the `-ccs` vendor suffix is published to Maven Central at **any** of the seven
versions checked, spanning both the published release and current `main`. There is no "just bump the
version" escape — and note that bumping is exactly what has been happening: `main` moved from
`7.9.5-ce`/`7.9.8` to `7.9.9-ce`/`7.9.9` while this specification was being written, with the defect
fully intact. Relocation to Apache coordinates is the only route for the two `org.apache.kafka`
artifacts, and there is no route at all for the two `io.confluent` ones — confirming R3's conclusion
that they must stop being inherited.

This is also the argument for making Contract C1 a standing assertion rather than a one-time fix:
nothing currently tests the property, so every automated bump lands green while keeping the artifact
unresolvable.

**Alternatives considered**:

- *Apache 4.x* (latest on Central is 4.3.1). Rejected: 4.x removes deprecated APIs and raises the
  minimum broker, which is a compatibility change this feature has no mandate to make.
- *Keep `-ce` and document the Confluent resolver in the README.* Rejected by FR-001/FR-002 — the
  published POM cannot carry a resolver (`pomIncludeRepository := { _ => false }`), so the artifact
  stays broken for anyone who does not read that specific README line before their first build.
- *Make the client non-inherited.* Rejected by the maintainer at specification time (FR-018): it would
  force every consumer, including plain-serialization users, to edit their build to upgrade.

---

## R2 — Does the plugin compile against the Apache coordinates?

**Decision**: Yes for source compatibility — but the spike also proved the build does **not** actually
resolve to the Apache version, so a resolution pin is required. See R3.

**Spike performed twice** (2026-08-09, this worktree, reverted after each run). Run 1 set
`Versions.kafka = "3.9.1"` against the branch point; run 2 set `Versions.kafka = "3.9.2"` against
current `main` (`4516572`, which carries `7.9.9-ce` / `7.9.9`). Both ran:

```bash
sbt -batch "clean; compile; Test/compile; evicted"
```

**Result, both runs**: exit 0. `compile` built 28 Scala + 16 Java main sources and `Test/compile` built
26 Scala + 4 Java test sources, under the project's `-Xfatal-warnings` setting, with **zero errors and
zero compiler warnings**. Changing the coordinates is source-compatible for both the main and test
source sets, and the result is stable across the dependency bump that landed mid-specification.

**But neither spike tested what it appeared to test.** Run 2's `evicted` report showed:

```text
[warn] * org.apache.kafka:kafka-clients:7.9.9-ccs is selected over {3.9.2}
[warn]     +- org.galaxio:gatling-kafka-plugin_2.13:… (depends on 7.9.9-ccs)
[warn]     +- io.confluent:kafka-schema-registry-client:7.9.9 (depends on 7.9.9-ccs)
[warn]     +- org.apache.kafka:kafka-streams:3.9.2 (depends on 3.9.2)
[warn]     +- org.apache.kafka:kafka-clients:3.9.2 (depends on 3.9.2)
```

Run 1 was identical with `7.9.8-ccs` over `{3.9.1}`. The two `io.confluent` artifacts pull
`kafka-schema-registry-client`, which depends on the vendor-suffixed client; sbt's version conflict
resolution selects the higher version and evicts the Apache one. **The spikes compiled against the
Confluent client, not the Apache one.** Their clean result is evidence that the coordinates are
declarable, not yet that the Apache client works.

A second, unrelated conflict appears for the same reason: `slf4j-api:2.0.17` (Apache 3.9.x) over
`1.7.36` (Confluent chain). The build already excludes `slf4j-api` from its direct Kafka dependencies,
so this is transitive-only.

**Follow-up required in implementation**: pin the client version so the build compiles and tests
against exactly what a consumer will resolve, then re-run the spike and confirm `evicted` reports no
`kafka-clients` conflict. Until that pin exists, no claim about Apache 3.9.2 behavior is supported by
evidence. This is captured as a plan task and as an explicit gate in [quickstart.md](./quickstart.md).

**Why the pin is needed even though the artifacts become non-inherited**: a non-inherited dependency is
still on the *plugin's own* compile and test classpath. Without a pin, the plugin would be built and
tested against the Confluent client while consumers run it against the Apache one — the exact
build/runtime mismatch that makes a green suite meaningless.

---

## R3 — Why the Confluent Avro artifacts must stop being inherited, and what that breaks

**Decision**: Move `io.confluent:kafka-streams-avro-serde` and `io.confluent:kafka-avro-serializer` to a
scope consumers do not inherit, matching the existing treatment of `avro4s-core`. There is no
alternative — R1 shows no Maven Central equivalent exists at any version.

**The obstacle**: the coupling is not confined to the Avro feature. Four sites in `src/main` reference
Confluent types, and the first two are reached by *every* simulation:

| Site | What it is | Reached by |
| --- | --- | --- |
| `request/KafkaSerdesImplicits.scala:45` | `implicit val avroSerde: Serde[GenericRecord] = new GenericAvroSerde()` | `Predef` — strict trait `val`, runs at object initialisation |
| `request/KafkaSerdesImplicits.scala:35-43` | `serdeClass[T]` using `KafkaAvroSerializer` / `CachedSchemaRegistryClient` | `Predef` — but a `def`, body deferred |
| `javaapi/checks/KafkaChecks.scala:26` | `val avroSerde: Serde[GenericRecord] = new GenericAvroSerde()` | Java DSL — strict object `val` |
| `javaapi/KafkaDsl.java:103,107` and `javaapi/expressions/Builders.java:72` | `SchemaRegistryClient` in public Java signatures | Java facade |

`Predef` is `object Predef extends KafkaDsl`, and `KafkaDsl extends KafkaCheckSupport with
KafkaSerdesImplicits`. So merely touching `Predef` initialises the trait and executes
`new GenericAvroSerde()`. Removing the artifact without addressing this converts today's
resolution-time failure into a load-time `NoClassDefFoundError` in the middle of a load test — strictly
worse for the consumer, and the reason FR-020 exists.

**Key distinction that makes the fix tractable**: the *declared types* in these members are
`Serde[T]` (kafka-clients, on Central) and `GenericRecord` (`org.apache.avro`, on Central). Only the
*initialiser expressions* name Confluent types. Signatures can therefore stay exactly as they are
while the Confluent construction is deferred.

**Approach — delegation, not deferral machinery**:

1. Add the opt-in entry point (R5). The Confluent constructions move there and live there. It is the
   only place in `src/main` that names a Confluent type for serdes.
2. The members staying on `KafkaSerdesImplicits` become one-line delegations to it, annotated
   deprecated. `implicit val avroSerde: Serde[GenericRecord] = new GenericAvroSerde()` becomes
   `implicit lazy val avroSerde: Serde[GenericRecord] = confluent.avroSerde`; `serdeClass[T]` delegates
   the same way and needs no `lazy`, being already a `def`.
3. `javaapi/checks/KafkaChecks.avroSerde` delegates identically.

**Why this is smaller than it first looked.** An earlier draft of this research proposed making the
`val`s lazy *and* isolating each construction behind a dedicated holder class, to be safe about when
the JVM verifier resolves a missing type. Delegation gets both properties for free: the holder already
exists — it is the opt-in object — and after delegation `KafkaSerdesImplicits` contains no reference to
a Confluent type at all, in signatures or in bytecode. There is nothing left to be subtle about, and
nothing to prove beyond the runtime check in R7. The whole change to the deprecated members is one
keyword and one annotation each.

`lazy` is still required on `avroSerde` for a second reason: without it, `Predef` initialisation would
force `object confluent` to initialise, which would construct the Confluent serde eagerly again through
one more hop.

**Why signature-level decoupling matters for Scala specifically**: implicit search reads the signature
of every implicit member of a trait in scope. Had a Confluent type appeared in one of those signatures,
plain consumers would fail to compile regardless of laziness. That is precisely the situation of the
two Kafka Streams implicits, which is why R4 reaches a different conclusion for them.

**Open risk carried into implementation** — Java overload resolution. `javaapi/KafkaDsl.java` declares
three `avro` overloads; two name `SchemaRegistryClient` and one takes `Serializer`/`Deserializer`
instead. The JVM resolves method descriptors lazily, so a consumer who never calls the Schema Registry
overloads should load and run fine. Whether **javac and kotlinc** can compile a call to the third
overload while the other two name a type absent from the classpath is not established and must be
tested, not assumed. If it fails, the `SchemaRegistryClient`-typed overloads move to the opt-in Java
class and the in-place ones cannot be retained — which would be a Java-source break needing its own
decision. Recorded as a decision gate in the plan.

---

## R4 — Why the Kafka Streams artifact stays inherited this release

**Decision**: Keep `kafka-streams-scala` inherited, relocated to Apache coordinates under R1 so it
satisfies FR-001. Deprecate the two implicits holding it; shed the artifact when they are removed.

**Rationale**: unlike the Avro members, these two have Kafka Streams types **in their signatures**:

```scala
implicit def sessionWindowedSerde[T](implicit tSerde: Serde[T]): WindowedSerdes.SessionWindowedSerde[T]
implicit def consumedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Consumed[K, V]
```

Scala implicit search reads these signatures for every simulation that imports the DSL. No amount of
lazy initialisation helps: the types must be on the classpath for the trait to be usable at all.
Keeping the implicits compiling for one more minor — required by Constitution Principle I — therefore
*requires* keeping the artifact. The two obligations are inseparable, which is what the maintainer
resolved in favour of the constitution at specification time (FR-019).

R1 makes this harmless: the Apache coordinate is on Maven Central, so the retained artifact satisfies
FR-001 like any other. Consumers see no change.

**Alternatives considered**: shedding now (rejected — would break compilation for every consumer, not
just users of the two implicits, since implicit search reads the signatures regardless of use);
removing them outright as a major (rejected — promotes a packaging fix to a major version bump).

---

## R5 — Shape and naming of the opt-in Avro entry point

**Decision**: A Scala `object org.galaxio.gatling.kafka.confluent`, imported as
`import org.galaxio.gatling.kafka.confluent._`, holding the Schema-Registry-backed serde surface. For
Java, a companion class carrying whichever entry points R3's verification shows cannot stay in place.

**Rationale**:

- The project already has exactly this pattern: `object org.galaxio.gatling.kafka.avro4s`, imported as
  `import org.galaxio.gatling.kafka.avro4s._`, which holds the avro4s serde and is documented as
  requiring a dependency the consumer adds. It depends only on Central-published artifacts, so it needs
  no change — and it is the precedent to copy rather than invent against.
- `confluent` rather than `avro` because the import name then matches the dependency the consumer must
  add, which is the association the migration guide needs them to make. `avro` would be ambiguous
  against the existing `avro4s` object and against plain Apache Avro, which does not require the
  opt-in.

**Consumer-visible change**: one import line for Schema-Registry Avro users, stated in the migration
guide. This is the change the maintainer approved under FR-020.

**Alternatives considered**: a mixin trait rather than an object (rejected — `avro4s` establishes the
object-with-import idiom and a trait invites re-mixing into a custom `Predef`, re-coupling it); reusing
the `avro4s` object (rejected — it would drag Confluent into a surface that is currently Central-clean).

---

## R6 — How to prove Maven Central-only resolvability

**Decision**: Two complementary gates, cheapest first.

1. **POM assertion (fast, always-on).** Assert on the output of `makePom`: no dependency in a scope
   consumers inherit may carry vendor-suffixed or `io.confluent` coordinates. This is deterministic,
   needs no network, and fails in seconds. It is also the gate that would have caught this defect
   originally.
2. **Scratch-project resolution (slow, CI).** Publish the plugin locally (`publishM2`), then for each
   of sbt, Gradle, and Maven generate a minimal consumer configured with **Maven Central plus the local
   repository only** — no Confluent resolver — and assert that resolution and compilation of a plain
   simulation succeed.

**Rationale**: gate 1 tests the published contract directly and runs on every build. Gate 2 tests the
consumer experience end to end and is the only thing that catches a build-tool-specific scope
difference — which FR-003 requires precisely because the three tools treat non-inherited scopes
differently and translating one to the others by analogy is how this class of defect survives.

**Why local publication is required**: the version under test is not on Central, so a genuine
Central-only consumer cannot resolve it. Adding the local repository is the minimum deviation, and it
does not weaken the test: the *transitive* dependencies still have to come from Central alone, which is
exactly the property under test.

**Alternatives considered**: sbt `scripted` tests. The infrastructure is not present (no `src/sbt-test`
directory; the recent "sbt, scripted-plugin" bump is Scala Steward tracking sbt's own version) and
scripted only covers the sbt path, leaving FR-003's Gradle and Maven requirements unmet. Rejected as
insufficient alone; a scripted harness remains a reasonable later refinement for the sbt path.

---

## R7 — Test-first strategy for a classpath change

**Decision**: The failing-first test is a **runtime check with the Confluent artifacts absent from the
classpath**: construct the DSL entry point and execute a plain produce and a plain request-reply
simulation against a real broker.

**Rationale**: Constitution Principle IV requires a test that fails before the change and passes after.
Against today's code this test fails at `Predef` initialisation with a `NoClassDefFoundError` for
`GenericAvroSerde` — the precise defect R3 describes — and passes once the initialisation is deferred.
Principle II is satisfied because the produce and request-reply paths run against Testcontainers rather
than a stub; the classpath is what varies, not the broker.

The POM assertion from R6 is the second failing-first test: it fails against the current build
definition, naming all four offending coordinates.

**Alternatives considered**: asserting on the build definition's text (rejected — tests the declaration,
not the outcome, and would have passed happily throughout the period this defect shipped); a
reflection-based check that the Confluent classes are unreferenced (rejected — proves less than simply
running without them).

---

## R8 — Deprecations under `-Xfatal-warnings`

**Decision**: Route every in-project usage of a newly deprecated member to its new home in the same
change; suppress only where a deprecated member's own definition or its regression test must reference
it.

**Rationale**: the build compiles with `-Xfatal-warnings`, so a deprecation the project itself trips
over turns the build red. An audit at spec time found no caller of either Kafka Streams implicit
anywhere in sources, tests, or docs, so those two are free. The Avro members do have in-project users
(examples and Java tests) which must move to the opt-in import — which has the useful side effect of
making the examples demonstrate the documented consumer path.

Two unused Kafka Streams imports in `src/test/.../examples/BasicSimulation.scala:11-12` are removed as
part of this (FR-016); they are currently unused-but-tolerated and would otherwise become noise.

**`since` value**: `@deprecated(..., "1.3.0")`. The build derives its version from the git tag via
dynver, so there is no version constant to reference; the string is written literally and must match
the release this ships in.

---

## R9 — What consumers inherit from the opt-in artifacts

**Finding, for documentation rather than decision**: `io.confluent:kafka-avro-serializer` declares a
dependency on `kafka_${scala.version}` — the Kafka **broker** artifact — alongside
`kafka-schema-registry-client`, `avro`, `guava`, `commons-compress` and `logredactor`. Consumers who opt
in inherit that whole chain, including a broker they will never run.

This is not a regression — the same chain is inherited today, in a worse form, because the artifact is
currently a compile-scope dependency. But since FR-008 requires the documentation to state exact
coordinates, the snippet should be written knowing what it drags in, and should show the broker
exclusion so that opting into Avro does not silently add tens of megabytes to a load-test classpath.

---

## Resolved unknowns summary

| # | Question | Outcome |
| --- | --- | --- |
| R1 | Replacement coordinates | Apache **3.9.2** on Central (newest 3.9.x); no vendor-suffixed build is on Central at any of the seven versions checked |
| R2 | Does it compile? | Yes, in two spikes, zero warnings under `-Xfatal-warnings` — but both resolved to the Confluent client, so a version pin is required before the result means anything |
| R3 | Can the Avro artifacts become optional? | Yes, by deferring two strict `val` initialisers; signatures are already Confluent-free. Java overload resolution is an open verification gate |
| R4 | Can Kafka Streams be shed now? | No — its types are in implicit signatures; it stays, relocated to Central |
| R5 | Opt-in entry point | `object org.galaxio.gatling.kafka.confluent`, mirroring the existing `avro4s` object |
| R6 | How to prove resolvability | POM assertion on every build, plus scratch-project resolution per build tool in CI |
| R7 | Test-first approach | Run plain simulations with the Confluent artifacts off the classpath; fails today at `Predef` init |
| R8 | Deprecations vs `-Xfatal-warnings` | No existing callers of the Streams implicits; Avro users in-project move to the opt-in import |
| R9 | Opt-in artifact weight | Pulls the Kafka broker artifact transitively; document an exclusion in the snippet |

**No unresolved NEEDS CLARIFICATION items remain.** Two items are carried into implementation as
verification gates rather than open questions: the client version pin (R2) and Java overload resolution
without Schema Registry on the classpath (R3).
