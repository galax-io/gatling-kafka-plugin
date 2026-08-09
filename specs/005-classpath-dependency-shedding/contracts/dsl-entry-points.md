# Contract: DSL Entry Points

**Feature**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09

The Scala DSL and the Java facade are a published contract (Constitution Principle I). This feature
changes no signature; it changes *what must be on the classpath* for each entry point to be reachable.
That is still an observable change to the contract, so it is specified here.

**Consumers of this contract**: Gatling simulations written in Scala, Java, and Kotlin against
`org.galaxio.gatling.kafka`.

---

## E1 — The default import is vendor-free

`import org.galaxio.gatling.kafka.Predef._` MUST be usable with only the inherited dependency set on
the classpath.

Concretely, with the `io.confluent` artifacts absent, all of the following MUST succeed:

1. Compiling a simulation that imports `Predef._`.
2. Initialising `Predef` at runtime.
3. Executing a plain produce simulation.
4. Executing a plain request-reply simulation with checks.

**Why all four are listed separately**: the current defect passes (1) and fails (2). A contract that
only said "compiles" would be satisfied by the broken state.

**Mechanism**: no member reachable from `Predef` may name a vendor-only type in its *declared type*, and
none may *construct* one during initialisation. The declared types are already compliant (`Serde[T]` and
`GenericRecord` are both Central-published). The two eager `val`s delegate to the opt-in object instead
of constructing, and become `lazy` so that delegating does not force that object to initialise. After
this, no file reachable from `Predef` names a Confluent type — in a signature or in bytecode.

**Test**: run (3) and (4) against a real broker with the Confluent artifacts removed from the runtime
classpath. Fails today with `NoClassDefFoundError: io/confluent/kafka/streams/serdes/avro/GenericAvroSerde`
raised from `Predef`'s initialiser.

---

## E2 — Schema Registry Avro has one documented entry point

Schema-Registry-backed Avro support is reachable via:

```scala
import org.galaxio.gatling.kafka.confluent._
```

This object is the canonical home of `avroSerde` and `serdeClass[T]`. It mirrors the existing
`org.galaxio.gatling.kafka.avro4s` object, which is the same idiom for the same reason — an optional
capability whose dependency the consumer declares.

**Naming rationale**: `confluent` matches the artifacts the consumer must add
(`io.confluent:kafka-avro-serializer`, `io.confluent:kafka-streams-avro-serde`), so the import and the
dependency reinforce each other in the migration guide. `avro` was rejected as ambiguous against both
`avro4s` and plain Apache Avro, neither of which needs the opt-in.

**Requirement**: everything reachable before this change stays reachable here, with identical behavior
(FR-009). This object adds no capability and changes no semantics.

---

## E3 — Deprecated members keep compiling

The following keep compiling in this release, each annotated with its replacement and its removal
release:

| Member | Replacement | Removed in |
| --- | --- | --- |
| `KafkaSerdesImplicits.avroSerde` | `org.galaxio.gatling.kafka.confluent.avroSerde` | 2.0.0 |
| `KafkaSerdesImplicits.serdeClass[T]` | `org.galaxio.gatling.kafka.confluent.serdeClass[T]` | 2.0.0 |
| `KafkaSerdesImplicits.sessionWindowedSerde[T]` | none — Kafka Streams is not part of this plugin | 2.0.0 |
| `KafkaSerdesImplicits.consumedFromSerde[K,V]` | none — as above | 2.0.0 |

**The last two name no replacement deliberately.** Constitution Principle I requires a replacement to be
named where one exists; inventing one here would imply the plugin offers a Kafka Streams capability it
does not. The annotation states that the plugin does not use them and that consumers writing Streams
topologies should depend on Kafka Streams directly.

**Deprecating `avroSerde` and `serdeClass` does not resurrect the coupling.** They remain on the trait
reachable from `Predef`, but as lazy delegations to the opt-in object rather than as constructions, so
E1 continues to hold. A consumer who ignores the deprecation and stays on the old member still needs the
opt-in artifacts at runtime — the deprecation moves the *documentation*, and the artifacts are what
actually gate the capability.

**Cost of retaining them**: one keyword and one annotation per member. This is worth stating because it
is what keeps the change source-compatible for Scala consumers; the only upgrade step for an Avro user
is in their build file, not their code.

**`-Xfatal-warnings` consequence**: every in-project use of these members moves to the opt-in import in
the same change, or the build goes red. This is desirable — it makes the examples demonstrate the
documented consumer path.

---

## E4 — The Java facade is usable without Schema Registry

A Java or Kotlin simulation using plain serialization MUST compile and run with the Confluent artifacts
absent.

**Status: provisional, pending gate G2.** `javaapi/KafkaDsl.java` declares three `avro` overloads, two
naming `SchemaRegistryClient` and one taking `Serializer`/`Deserializer`. The JVM resolves method
descriptors lazily, so runtime is expected to be fine for a consumer who never calls the Schema Registry
overloads. Compile-time behavior under overload resolution is **not established**: javac and kotlinc may
require the parameter types of every candidate `avro` overload to be on the classpath.

Two outcomes:

- **G2 passes** — the `SchemaRegistryClient`-typed overloads and the `AvroExpressionBuilder` constructor
  stay in place, deprecated, pointing at the opt-in Java entry point. Java source compatibility is
  fully preserved.
- **G2 fails** — they must move out of `KafkaDsl`, which is a Java-source break with no deprecation
  window. That changes the release's compatibility story and must be raised with the maintainer before
  implementation, not decided inside it.

**Test**: compile a plain Java simulation and a plain Kotlin simulation against the published artifact
with Confluent absent, exercising `avro(JExpression, Serializer, Deserializer)` so overload resolution
is actually forced.

---

## E5 — Unchanged surface

Explicitly out of scope and required to be byte-identical in behavior: the 15 primitive serde implicits,
`kafka(...)` protocol and request builders, all check builders other than the Avro ones, request-reply
correlation, tracker and consumer behavior, and every `org.galaxio.gatling.kafka.avro4s` member.

**Test**: `ExampleSmokeValidation` constructs every README and example simulation; the existing unit,
integration, and Gatling suites pass unchanged. No suite may be relaxed or disabled to accommodate this
feature.

---

## Contract test summary

| ID | Gate | Fails today | Needs broker |
| --- | --- | --- | --- |
| E1 | Default import usable without vendor artifacts | **yes**, at runtime | yes |
| E2 | Opt-in entry point exists and is complete | **yes** (does not exist) | yes, for the Avro path |
| E3 | Deprecated members compile, annotations correct | **yes** (not annotated) | no |
| E4 | Java/Kotlin facade usable without Schema Registry | **unknown** — gate G2 | no |
| E5 | Everything else unchanged | no | yes |
