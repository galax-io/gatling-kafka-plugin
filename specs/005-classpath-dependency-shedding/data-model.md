# Phase 1 Data Model: Classpath and Dependency Shedding

**Feature**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09

This feature has no runtime data model. Its "entities" are the classpath surfaces named in the spec's
Key Entities section: what the published artifact declares, and what each declaration obliges of a
consumer. Modelling them explicitly is what makes the acceptance criteria checkable — every rule below
is something a test can assert.

---

## Entity: Dependency Declaration

One entry in the published build definition.

| Attribute | Values | Notes |
| --- | --- | --- |
| `coordinates` | group : artifact : version | The identity a consumer's build resolves |
| `inheritance` | `inherited` \| `opt-in` \| `build-only` | Whether a consumer acquires it by declaring the plugin |
| `origin` | `central` \| `vendor-only` | Where the artifact can actually be fetched from |
| `justification` | `used-by` *code path* \| `retained-for` *deprecation* | Why it is declared at all |

### Validation rules

- **DR-1**: `inheritance = inherited` ⟹ `origin = central`. This is FR-001 and the whole of the S1
  defect. Four declarations violate it today.
- **DR-2**: `origin = vendor-only` ⟹ `inheritance ∈ {opt-in, build-only}`. The contrapositive of DR-1,
  stated the way the fix is applied.
- **DR-3**: Every declaration has a non-empty `justification`. A declaration justified by
  `retained-for` must name the release in which the deprecation resolves.
- **DR-4**: At most one declaration may carry `retained-for` in this release — the Kafka Streams
  artifact. This is SC-007's "exactly one recorded exception", stated as an assertable bound rather
  than a prose caveat.
- **DR-5**: A declaration that the build never applies is not a declaration; it is dead text and is
  removed (FR-015). The `avroCompiler` entry in `project/Dependencies.scala` is defined but never added
  to `libraryDependencies` and falls here.

### Transitions

| Coordinate | Current on `main` | After (1.3.0) | Later (2.0.0) |
| --- | --- | --- | --- |
| `org.apache.kafka:kafka-clients` | inherited, vendor-only `7.9.9-ce` | inherited, central `3.9.2` | unchanged |
| `org.apache.kafka:kafka-streams-scala` | inherited, vendor-only `7.9.9-ce` | inherited, central `3.9.2`, `retained-for` the deprecated implicits | **removed** |
| `io.confluent:kafka-streams-avro-serde` | inherited, vendor-only `7.9.9` | **opt-in**, vendor-only | unchanged |
| `io.confluent:kafka-avro-serializer` | inherited, vendor-only `7.9.9` | **opt-in**, vendor-only | unchanged |
| `com.sksamuel.avro4s:avro4s-core` | opt-in, central | unchanged | unchanged |
| `org.apache.avro:avro` | inherited, central | unchanged | unchanged |
| `org.apache.avro:avro-compiler` | declared, never applied | **removed** | — |

The `avro4s-core` row is the reference implementation of the target state: opt-in, documented, and
already correct. It is included to make the point that this feature applies an existing pattern rather
than inventing one.

The version strings in the first column are a snapshot — dependency automation moves them regularly
(the four vendor coordinates advanced from `7.9.5-ce`/`7.9.8` to `7.9.9-ce`/`7.9.9` during
specification). Every rule above is stated over `inheritance` and `origin`, never over a version, so a
bump changes this column and nothing else.

---

## Entity: DSL Entry Point

One published Scala or Java member a simulation can reach.

| Attribute | Values |
| --- | --- |
| `surface` | `plain` \| `avro-optin` |
| `location` | `default-import` \| `optin-import` |
| `coupling` | `none` \| `signature` \| `initialiser` |
| `status` | `current` \| `deprecated(since, removed-in, replacement)` |

### Validation rules

- **EP-1**: `location = default-import` ⟹ `coupling ≠ signature` for any vendor-only type. A vendor type
  in a signature is read by Scala implicit search for every consumer, so laziness cannot rescue it.
  This is the rule that forces the Kafka Streams artifact to stay (R4).
- **EP-2**: `location = default-import` ∧ `coupling = initialiser` ⟹ the initialiser is deferred and
  isolated, so that loading and initialising the default entry point executes no vendor code. This is
  FR-005, and the rule the two eager `val`s break today.
- **EP-3**: Every `avro-optin` entry point is reachable from the opt-in import. Nothing is only
  reachable from a deprecated location.
- **EP-4**: `status = deprecated` ⟹ the member still compiles, and its annotation names both the
  replacement and the removal release (Constitution Principle I).
- **EP-5**: No entry point is removed in this release.

### Current state and target

| Entry point | Surface | Coupling today | Action |
| --- | --- | --- | --- |
| `KafkaSerdesImplicits.avroSerde` | avro-optin | initialiser, **eager** — breaks EP-2 | Defer + isolate; deprecate in place; canonical copy in the opt-in object |
| `KafkaSerdesImplicits.serdeClass[T]` | avro-optin | initialiser, already deferred (`def`) | Deprecate in place; canonical copy in the opt-in object |
| `KafkaSerdesImplicits.sessionWindowedSerde[T]` | plain (unused) | **signature** — permitted only because the artifact stays inherited | Deprecate; artifact retained under DR-4 |
| `KafkaSerdesImplicits.consumedFromSerde[K,V]` | plain (unused) | **signature** — as above | Deprecate; artifact retained under DR-4 |
| The 15 primitive serde implicits | plain | none | Unchanged |
| `javaapi.checks.KafkaChecks.avroSerde` | avro-optin | initialiser, **eager** — breaks EP-2 | Defer + isolate |
| `javaapi.KafkaDsl.avro(Object, SchemaRegistryClient)` | avro-optin | **signature** (Java) | Deprecate in place, pending gate G2 |
| `javaapi.KafkaDsl.avro(JExpression, SchemaRegistryClient)` | avro-optin | **signature** (Java) | Deprecate in place, pending gate G2 |
| `javaapi.KafkaDsl.avro(JExpression, Serializer, Deserializer)` | avro-optin | none | Unchanged — the overload G2 must prove still compiles |
| `javaapi.KafkaDsl.avroBody(...)` | avro-optin | initialiser via `KafkaChecks.avroSerde` | Unchanged once EP-2 is satisfied — its signature is already Central-clean |
| `javaapi.expressions.Builders.AvroExpressionBuilder(…, SchemaRegistryClient)` | avro-optin | **signature** (Java) | Deprecate in place, pending gate G2 |
| `object avro4s` members | avro-optin (avro4s) | none | Unchanged — already correct |

**Why Java signature coupling is treated differently from Scala.** EP-1 is a Scala rule because
implicit search reads member signatures eagerly. The JVM resolves method descriptors lazily, so a Java
signature naming an absent type is tolerable at runtime for a consumer who never calls it. Whether it
is tolerable at *compile* time under overload resolution is gate G2, and the table above is provisional
on it.

---

## Entity: Consumer Configuration

What a consumer must have in place for a given capability. This is the entity the documentation
describes and the scratch-project checks instantiate.

| Capability | Repositories | Declarations | Imports |
| --- | --- | --- | --- |
| Plain serialization | Maven Central | the plugin | default DSL import |
| Avro via avro4s | Maven Central | the plugin, `avro4s-core` | default + `avro4s` import |
| Avro via Schema Registry | Maven Central **+ Confluent** | the plugin, the two `io.confluent` artifacts | default + **opt-in `confluent` import** |

### Validation rules

- **CC-1**: The plain row requires no repository beyond the default. This is SC-002, and it is the row
  the scratch-project checks assert for all three build tools (FR-003).
- **CC-2**: Each non-default row is fully stated in the installation documentation — exact coordinates
  and exact repository URL, per build tool (FR-008). "Fully" means a consumer needs no source outside
  the README.
- **CC-3**: Moving between rows is additive. No row requires removing anything another row needs, so a
  consumer can adopt Avro without unwinding a plain setup.
- **CC-4**: The Schema Registry row's declarations pull the Kafka broker artifact transitively (R9);
  the documented snippet shows the exclusion so opting into Avro does not silently add a broker to a
  load-test classpath.

### Upgrade transitions from 1.2.x

| Consumer | Change required | Discoverable from |
| --- | --- | --- |
| Plain serialization | **none** | — (SC-006) |
| Avro via avro4s | none | — |
| Avro via Schema Registry, Central-only | was already broken; now works by following the documented row | Installation section |
| Avro via Schema Registry, with a self-configured Confluent resolver | add two declarations; add one import | **Migration Guide** — this is the only population whose working setup changes, and the reason FR-011 is mandatory |
