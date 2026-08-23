# Contract: Surviving DSL Surface

**Feature**: `006-v2-cleanup-sweep` | **Requirements**: FR-001, FR-003, FR-004, FR-007, FR-014, FR-015,
FR-016

A removal sweep is specified by what goes. It is *verified* by what stays. This contract is the
positive statement — the surface that must still exist and still behave after every deletion in this
feature, and the properties that must survive the two simplifications in US3.

It amends `specs/005-classpath-dependency-shedding/contracts/dsl-entry-points.md`, which remains in
force except where noted.

---

## S1 — Every reachable `send` can send

**Contract**: after this feature, every `send(...)` reachable from the Scala DSL or the Java facade
produces an action that carries a producer topic.

Surviving shapes:

| Path | Form |
|---|---|
| Produce-only, Scala | `kafka(name).topic(t).send(value)` / `.send(key, value[, headers])` |
| Produce-only, Java | `kafka(name).topic(t).send(...)` — the `OnlyPublishStep` matrix |
| Request-reply, Scala | `kafka(name).requestReply.requestTopic(rt).replyTopic(ct).send(key, value[, headers])` |
| Request-reply, Java | `kafka(name).requestReply().requestTopic(rt).replyTopic(ct).send(...)` |

**Verification**: `ExampleSmokeValidation` constructs every README and example simulation; both CI
Gatling simulations run end to end against the Compose stack; all four Kotlin examples use only these
shapes (checked during research) and must still compile in a scratch project.

**Consequence for the implementation**: `KafkaAction`'s `missingProducerTopicError` path becomes
unreachable once no builder can produce `producerTopic = None`. It is removed with the family — leaving
a diagnostic for a state that cannot occur is the same class of residue this feature exists to clear.

## S2 — Reply failures still name their cause

**Contract**: removing `KafkaProtocolMessage.responseCode` MUST NOT change what a report shows for a
failed request.

The reporting slot is populated by `KafkaRequestReplyAction.failureType` and
`KafkaMessageTracker.failPending` with the exception's own type — `TimeoutException`,
`RecordTooLargeException`, and so on. That is a different source from the removed field, which has only
ever been `None`.

**Verification**: the request-reply integration specs assert reported failure causes; they must pass
unchanged. `KafkaLoggingSpec` is updated to the trace line's new exact text (data-model KM-4).

## S3 — One Avro body check path

**Contract**: exactly one Avro body check path exists, and both entry points reach it.

```text
KafkaCheckSupport.avroBody[T]  ─┐
                                ├─→ AvroBodyCheckBuilder._avroBody ─→ kafkaStatusCheck
KafkaDsl.avroBody()            ─┘
```

The preparer-based alternative (`KafkaCheckMaterializer.avroBody`, `KafkaMessagePreparer.avroPreparer`)
is removed — verdict B1, no caller anywhere.

**Property that must survive**: an Avro check against a reply with **no payload** reports the absent
payload by name, not Gatling's generic "found nothing". That guard lives in the extractor, via
`KafkaMessagePreparer.withPayload`, and `withPayload` is retained — it also serves the string, JSON,
JMESPath and XML preparers.

## S4 — Optional artifacts stay optional (amends 005 Contract E1)

**Contract**: initialising any DSL entry point MUST NOT construct a Confluent Schema Registry type.
Entry points covered, unchanged from 005:

- `org.galaxio.gatling.kafka.Predef$`
- `org.galaxio.gatling.kafka.javaapi.KafkaDsl`
- `org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks$`
- `org.galaxio.gatling.kafka.javaapi.request.expressions.Builders$AvroExpressionBuilder`

**What changes**: 005 satisfied this for `avroSerde` with `LazyGenericAvroSerde`, a deferring wrapper
needed because the published trait ABI forced a strict `val`. This feature satisfies it with
`implicit def avroSerde: Serde[GenericRecord] = ConfluentSerdes.newAvroSerde()` — a `def` body runs only
when a `Serde[GenericRecord]` is summoned, so there is nothing to defer.

**The 005 note that `avroSerde` MUST stay a strict `val` is superseded.** Its reasoning was specific to
a minor release: turning the member `lazy` deletes the mixin setter from the compiled interface, so a
simulation compiled against ≤1.3.0 reads `null` with no linkage error. A major release is where that
hazard is paid for openly, and a `def` avoids the setter question entirely rather than trading it.

**Verification (FR-015)**: `PlainClasspathIsolationSpec` keeps all four entry-point cases and its
positive control, and its `LazyGenericAvroSerde` case is **re-pointed, not deleted**. The re-pointed
test must distinguish:

- summoning `avroSerde` under a Confluent-denying loader — **must fail**, because the `def` body
  constructs immediately on summon; versus
- initialising the enclosing entry point — **must succeed**, because nothing constructs at init.

This is a real change in where the failure lands and the test must assert the new boundary, not be
relaxed to accept either.

## S5 — `send` returns the concrete builder

**Contract**: `send(...)` returns `KafkaRequestBuilder[K, V]`. The `RequestBuilder[K, V]` trait is
removed; the implicit conversion to Gatling's `ActionBuilder` in `KafkaDsl` is re-pointed at the
concrete type, and the Java `javaapi.request.builder.RequestBuilder` wrapper — a different class, in a
different package — wraps the concrete type instead.

**Break to record**: user code that names `RequestBuilder[K, V]` as a declared type stops compiling.
This is a cascade under `contracts/removed-api.md` R3 and needs a migration-guide entry.

## S6 — Consume-scoped timeout controls stay

**Contract**: `KPConsumeSettingsStep.timeout(...)` and `.withDefaultTimeout` remain, in both the Scala
DSL and the Java facade. Only the **producer**-scoped pair on the Scala `KPProducerSettingsStep` is
removed (verdict A5).

**Evidence this is the right cut**: the single `.withDefaultTimeout` in the example suite
(`examples/KafkaGatlingTest.scala`) follows `.consumeSettings(...)`, so it binds to the consume step.
The Java `KPProducerSettingsStep` never had timeout methods at all.

## S7 — Check types

**Contract**: `KafkaCheckType` retains `Simple`. `ResponseCode` is removed, and the duplicate branch in
`KafkaChecks.toScalaCheck` collapses — both arms built through `kafkaStatusCheck`, so the collapse
changes no behaviour for any check a user can construct.

`KafkaDsl.simpleCheck` is the only producer of a Kafka `CheckBuilder`, and it already returns `Simple`.
