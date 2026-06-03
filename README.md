# Gatling Kafka Plugin

[![CI](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.galaxio/gatling-kafka-plugin_2.13.svg?color=success)](https://search.maven.org/search?q=org.galaxio.gatling-kafka)
[![codecov](https://codecov.io/github/galax-io/gatling-kafka-plugin/coverage.svg?branch=main)](https://codecov.io/github/galax-io/gatling-kafka-plugin?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Kafka protocol plugin for [Gatling](https://gatling.io/) load testing framework. The `main` branch supports produce-only and request-reply Kafka flows with plain serialization and Avro helpers.

## Table of Contents

- [Compatibility](#compatibility)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Current API Surface](#current-api-surface)
- [Producing Messages](#producing-messages)
- [Request-Reply](#request-reply)
- [Runtime Semantics & Troubleshooting](#runtime-semantics--troubleshooting)
- [Consume-Only Tracking](#consume-only-tracking)
- [Avro Support](#avro-support)
- [Architecture](#architecture)
- [Migration Guide](#migration-guide)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Compatibility

| Branch / Line | Gatling | Scala | Java |
|---|---|---|---|
| `main` / `1.0.x` | 3.13.5 | 2.13.16 | 17+ |
| 0.22.x | 3.13.x | 2.13 | 17+ |
| 0.21.x | 3.12.x | 2.13 | 17+ |
| 0.20.4 | 3.11.5 | 2.13 | 17+ |

> **Version guidance:** if you are on Gatling `3.11.5`, use plugin `0.20.4`. The `1.0.x` / `main` line targets Gatling `3.13.x`.
>
> **Upgrading from an older release?** Start with the [Migration Guide](#migration-guide) below. It summarizes the supported upgrade paths and the breaking or behavioral changes that tend to matter most.
>
> **Branch strategy:** `main` is the active development branch and current release line. Short-lived topic branches are cut from `main`, and `backport/*` branches are only created when a released line needs a targeted follow-up fix.

## Installation

### Scala (sbt)

```scala
libraryDependencies += "org.galaxio" %% "gatling-kafka-plugin" % "<version>" % Test
```

### Java / Kotlin (Gradle Kotlin DSL)

```kotlin
gatling("org.galaxio:gatling-kafka-plugin_2.13:<version>")
```

### Maven

```xml
<dependency>
  <groupId>org.galaxio</groupId>
  <artifactId>gatling-kafka-plugin_2.13</artifactId>
  <version>${version}</version>
  <scope>test</scope>
</dependency>
```

If you are installing this while upgrading an older test suite, read the [Migration Guide](#migration-guide) before copying examples from `main`.

## Quick Start

### Docker (local Kafka)

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Stop:

```bash
docker compose -f docker-compose.kafka.yml down
```

### Minimal Scenario — Scala

```scala
import org.galaxio.gatling.kafka.Predef._
import io.gatling.core.Predef._

class KafkaSimulation extends Simulation {
  val kafkaConf = kafka
    .properties(Map("bootstrap.servers" -> "localhost:9092"))

  val scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
        .topic("test-topic")
        .send[String, String]("key", """{"msg": "hello"}""")
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaConf)
}
```

### Minimal Scenario — Java

```java
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.*;
import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaSimulation extends Simulation {
  var kafkaConf = kafka()
    .properties(Map.of("bootstrap.servers", "localhost:9092"));

  var scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
        .topic("test-topic")
        .send("key", "{\"msg\": \"hello\"}")
    );

  { setUp(scn.injectOpen(atOnceUsers(1)).protocols(kafkaConf)); }
}
```

### Minimal Scenario — Kotlin

```kotlin
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.*
import io.gatling.javaapi.core.CoreDsl.*

class KafkaSimulation : Simulation() {
  val kafkaConf = kafka()
    .properties(mapOf("bootstrap.servers" to "localhost:9092"))

  val scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
        .topic("test-topic")
        .send("key", """{"msg": "hello"}""")
    )

  init { setUp(scn.injectOpen(atOnceUsers(1)).protocols(kafkaConf)) }
}
```

## Current API Surface

The `main` branch currently ships:

- Produce-only actions via `kafka("name").topic("topic").send(...)`
- Request-reply actions via `kafka("name").requestReply.requestTopic(...).replyTopic(...).send(...)`
- Reply correlation configured at the protocol level with `.matchByValue` or `.matchByMessage(...)`
- Avro helpers via `org.galaxio.gatling.kafka.avro4s._` or custom Kafka `Serde[T]`

The following APIs are not available on `main` and are intentionally not documented below:

- Consume-only DSL methods such as `consumeFrom`, `consumeAny`, `keyForTracking`, or `saveAs`
- Per-action reply matcher overrides such as `requestMatchBy` and `replyMatchBy`
- Produce builder methods such as `partition`, `timestamp`, or `silent`
- ScalaPB / `KafkaProtobufDsl` helpers such as `protobufBody`

## Producing Messages

### Basic Send

```scala
import org.galaxio.gatling.kafka.Predef._

scenario("Producer")
  .exec(
    kafka("send string")
      .topic("test-topic")
      .send[String, String]("key", "payload"),
  )
```

### Partition and Timestamp Control

Target a specific partition or set an explicit timestamp on produced records:

```scala
kafka("send to partition")
  .topic("test-topic")
  .send[String, String]("key", "payload")
  .partition(3)
  .timestamp(System.currentTimeMillis())
```

Both `.partition()` and `.timestamp()` accept Gatling `Expression` values for dynamic resolution from the session.

### Silent Requests

```scala
kafka("silent request")
  .topic("test-topic")
  .send[String]("foo")
  .silent
```

Set the topic on each request builder with `kafka("name").topic("...")`.

---

## Request-Reply

Request-reply needs both producer settings and consumer settings. The producer sends the request, and the consumer side tracks replies on the configured reply topic.

```scala
import scala.concurrent.duration._

val kafkaConf = kafka
  .producerSettings(
    "bootstrap.servers" -> "localhost:9092",
  )
  .consumeSettings(
    "bootstrap.servers" -> "localhost:9092",
  )
  .timeout(10.seconds)
```

```scala
kafka("request reply").requestReply
  .requestTopic("requests")
  .replyTopic("replies")
  .send[String, String]("key", """{"action": "process"}""")
  .check(jsonPath("$.status").is("ok"))
```

### End-to-End Quick Start

The example below is the shortest complete setup we recommend for a new request-reply simulation on local Kafka.

```scala
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._

import scala.concurrent.duration._

class RequestReplySimulation extends Simulation {

  private val requestTopic = "requests"
  private val replyTopic   = "replies"

  private val kafkaConf = kafka
    .producerSettings(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ProducerConfig.ACKS_CONFIG              -> "1",
    )
    .consumeSettings(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG          -> s"gatling-rr-${System.currentTimeMillis()}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    )
    .timeout(15.seconds)

  private val scn: ScenarioBuilder = scenario("request-reply")
    .exec(
      kafka("send request").requestReply
        .requestTopic(requestTopic)
        .replyTopic(replyTopic)
        .send[String, String]("order-42", """{"action":"process"}""")
        .check(jsonPath("$.status").is("ok")),
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaConf)
}
```

Required consumer-side settings in that example:

- `consumeSettings("bootstrap.servers" -> ...)` is mandatory. Without it, the plugin never creates the reply-tracking consumer.
- `group.id` should be unique per local run unless you deliberately want to resume committed offsets.
- `auto.offset.reset=latest` keeps a fresh local group focused on replies produced after the simulation starts.
- `.timeout(...)` must cover both Kafka round-trip latency and the first consumer-group assignment for the reply topic.

Reply-topic assumptions:

- The system under test reads requests from `requestTopic`.
- The responder publishes correlated replies to `replyTopic`.
- By default, correlation matches request key to reply key. In the example above, both sides must use `order-42` as the Kafka key.

Minimal local responder setup:

1. Start Kafka locally with `docker compose -f docker-compose.kafka.yml up -d`.
2. Start a lightweight responder that consumes `requests` and republishes to `replies` using the same key.
3. Run the Gatling simulation and verify that the check passes.

If you want a repository-backed responder example instead of writing your own, see [KafkaIntegrationSpec.scala](src/test/scala/org/galaxio/gatling/kafka/integration/KafkaIntegrationSpec.scala), especially the request-reply integration test that wires an input topic, reply topic, sender, and dynamic consumer together end to end.

Expected success signal:

- Gatling marks the `send request` action as successful.
- The reply payload reaches the `.check(...)` clause.
- You do not see `Timed out waiting for reply` or `Timed out waiting for consumer assignment` errors in the run output.

### Matching Strategies

| Method | Request extractor | Response extractor |
|--------|------------------|--------------------|
| *(default)* | `msg.key` | `msg.key` |
| `.matchByValue` | `msg.value` | `msg.value` |
| `.matchByMessage(fn)` | `fn(msg)` | `fn(msg)` |

These matchers are configured on the protocol, not on individual request builders:

```scala
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

def correlationIdFromHeader(headerName: String): KafkaProtocolMessage => Array[Byte] =
  _.headers
    .flatMap(headers => Option(headers.lastHeader(headerName)).map(_.value()))
    .getOrElse(Array.emptyByteArray)

## Runtime Semantics & Troubleshooting

### What happens at runtime

- Request-reply uses a shared `KafkaConsumer` for reply tracking. The consumer is created once per distinct consumer `bootstrap.servers` value and reused by all scenarios using that protocol.
- A tracker actor is created per reply topic. The first request for a reply topic adds that topic to the shared consumer subscription and waits up to the protocol timeout for partition assignment.
- Correlation is in-memory. Each sent request stores its match id in the tracker until either a matching reply arrives or the timeout scanner marks it as failed.
- The protocol timeout is used in two places: as the reply deadline recorded for each request, and as the wait budget while a newly used reply topic is being assigned to the shared consumer.
- Cleanup happens only when Gatling terminates its actor system. Trackers, subscriptions, and the shared consumer stay alive for the life of the simulation and are not reset between scenarios.

### Consumer defaults injected by the plugin

When you supply `consumeSettings`, the plugin always adds byte-array deserializers and also injects these defaults unless you override them:

| Setting | Default | Why |
|---|---|---|
| `group.id` | `gatling-kafka-test-<uuid>` | Generated when absent so reply tracking can start without forcing a shared consumer group across runs. |
| `auto.offset.reset` | `latest` | New consumer groups start from newly produced replies instead of replaying old traffic. |
| `enable.auto.commit` | `true` | Kafka commits offsets automatically unless you opt out explicitly. |

Two important consequences follow from those defaults:

- `auto.offset.reset=latest` only matters when the consumer group has no committed offsets yet.
- If you set a fixed `group.id` and keep `enable.auto.commit=true`, later runs resume from committed offsets for that group. In that case Kafka may ignore `latest` and continue from the stored position instead.

### Operational guidance

- For isolated test runs, let the plugin generate `group.id` values or provide a unique `group.id` per run.
- For repeatable offset behavior with a fixed `group.id`, decide explicitly whether you want committed offsets. Override `enable.auto.commit` and `auto.offset.reset` instead of relying on defaults.
- Set the protocol timeout high enough to cover both reply latency and initial consumer-group assignment on the first request to each reply topic.
- Keep request and reply matchers aligned. The default matches on message key; `.matchByValue` and `.matchByMessage(...)` must extract the same logical id on both sides.

### Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| Requests are sent but no replies are ever matched | No consumer was created for tracking | Make sure the protocol includes `consumeSettings("bootstrap.servers" -> ...)`, not only producer settings. |
| First requests on a reply topic time out under load or right after startup | Topic subscription and partition assignment consumed most of the timeout budget | Increase `.timeout(...)` and verify the consumer group can join and get assignments promptly. |
| Replies seem to be skipped on later test runs | A reused `group.id` resumed from committed offsets | Use a fresh `group.id`, or override `enable.auto.commit` / `auto.offset.reset` deliberately. |
| Late replies do not recover a timed-out request | Correlation entries are removed after timeout | Treat the timeout as a hard deadline and size it for your end-to-end latency envelope. |
| Replies arrive on Kafka but still do not match | Request and reply are extracting different correlation ids | Verify whether you are matching by key, value, or a custom extractor, and confirm both sides produce the same bytes. |

---

## Consume-Only Tracking

```scala
kafka("consume event")
  .consumeFrom("events")
  .keyForTracking("#{eventKey}")
  .check(bodyString.exists)
  .saveAs("eventBody")(msg => new String(msg.value))
```

Consume first available (no correlation):

```scala
kafka("consume any")
  .consumeAny("events")
  .saveAs("payload")(msg => new String(msg.value))
```

---

## Avro Support

### Avro4s (Scala case classes)

Add avro4s to your test dependencies:

```scala
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.2" % Test
```

Usage with automatic schema derivation:

```scala
import com.sksamuel.avro4s._
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.avro4s._

case class Ingredient(name: String, sugar: Double, fat: Double)

scenario("Avro4s")
  .exec(
    kafka("send avro")
      .topic("ingredients")
      .send[String, Ingredient]("key", Ingredient("Cheese", 0d, 70d)),
  )
```

### Schema Registry Integration

For Schema Registry-backed Avro classes, provide an implicit `schemaRegUrl` or your own Kafka `Serde[T]`:

```scala
implicit val schemaRegUrl: String = "http://localhost:8081"
```

### Avro in Request-Reply

See [AvroClassWithRequestReplySimulation.scala](src/test/scala/org/galaxio/gatling/kafka/examples/AvroClassWithRequestReplySimulation.scala) for a complete request-reply example with a custom Avro `Serde`.

### Avro Schema Download

Using [sbt-schema-registry-plugin](https://github.com/galax-io/sbt-schema-registry-plugin):

```bash
sbt schemaRegistryDownload
```

## Architecture

```
Predef / KafkaDsl                 (entry points, implicits)
    |
KafkaProtocolBuilder              (producerSettings, consumeSettings, timeout, matchers)
KafkaRequestBuilderBase           (DSL: .topic.send, .requestReply)
    |
    +-- KafkaRequestAction              (produce-only action)
    +-- KafkaRequestReplyAction         (produce + correlated reply tracking)
    +-- KafkaConsumeAction              (consume-only tracking)
    |
KafkaMessageTrackerActor               (Akka actor for correlation)
TrackersPool                            (shared consumer per bootstrap servers, tracker per reply topic)
KafkaSender / KafkaSenderPool           (producer pool)
```

---

## Migration Guide

Use this section as release-based upgrade notes. Start from the version you are on today, then apply the checklist for the target line you want to adopt.

### Supported upgrade paths

| Current line | Target line | Notes |
|---|---|---|
| `0.22.x` / RC | `1.0.0` | Remove protocol-level `.topic(...)` calls, set topic on each request builder. Remove any use of `messageCheck`. |
| `0.20.4` | `1.0.x` | Move from Gatling `3.11.5` to `3.13.x`, update request-reply consumer settings, re-check examples against current README. |
| `0.21.x` | `1.0.x` | Stay on Gatling `3.13.x`, review request-reply defaults and DSL surface. |
| `0.20.x` or older | `1.0.x` | Treat as full doc refresh. Older consume-only or per-action matcher APIs are not present. |

### Upgrading to `1.0.0` from `0.22.x` / RC

#### Protocol-level topic API removed

The `kafka.topic("...")` shorthand on the protocol builder was deprecated in `1.0.0-RC1` and is now removed.

| Before (removed) | After |
|---|---|
| `kafka.topic("my-topic").properties(Map(...))` | `kafka.producerSettings(Map(...))...` |
| `kafka("req").send(payload)` with protocol-level topic | `kafka("req").topic("my-topic").send(payload)` |

Every request builder must now declare its own topic with `.topic("...")` or `.requestTopic("...").replyTopic("...")`.

#### `KafkaMessageCheck` removed

`messageCheck` accessor removed from the DSL. Use `simpleCheck { msg => ... }` or the standard `jsonPath` / `bodyString` check builders directly.

---

### Upgrading to `main` / `1.0.x`

#### Request-reply runtime moved from `KafkaStreams` to `KafkaConsumer`

The plugin uses `KafkaConsumer` instead of `KafkaStreams` for reply tracking.

| Before (Streams) | After (Consumer) |
|---|---|
| `application.id` | `group.id` |
| `default.key.serde` | _(removed)_ |
| `default.value.serde` | _(removed)_ |

```scala
// Before
.consumeSettings(Map(
  "bootstrap.servers" -> "localhost:9092",
  "application.id" -> "my-test-group",
))

// After
.consumeSettings(Map(
  "bootstrap.servers" -> "localhost:9092",
  "group.id" -> "my-test-group",
))
```

What to revisit during this step:

- Remove obsolete Streams-only config such as `default.key.serde` and `default.value.serde`.
- Treat `group.id` as a runtime behavior choice, not just a rename. Reusing the same group means later runs may resume committed offsets.
- Make sure your request-reply protocol actually includes `consumeSettings(...)`; producer settings alone are not enough.

#### Current examples should replace stale snippets

Older snippets often show only `requestTopic(...)` and `replyTopic(...)`, but upgrade work should also refresh the surrounding consumer configuration and timeout choices. When moving to `main`, review the current README examples instead of copying older request-reply fragments blindly.

#### API surface on `main` is narrower than some older examples

Before upgrading old simulations, compare them against [Current API Surface](#current-api-surface). In particular, `main` intentionally does not document or expose older patterns such as:

- consume-only DSL calls like `consumeFrom`, `consumeAny`, `keyForTracking`, or `saveAs`
- per-action matcher overrides such as `requestMatchBy` / `replyMatchBy`
- ScalaPB helpers such as `KafkaProtobufDsl` / `protobufBody`

If your older suite depends on those APIs, plan a code migration instead of a pure version bump.

### Upgrade checklist

- Confirm your target line from the [Compatibility](#compatibility) table before changing dependencies.
- Update request-reply protocols to include both producer and consumer settings.
- Replace `application.id` with `group.id` if you are migrating from older `KafkaStreams`-based tracking.
- Decide whether your runs should reuse offsets or start fresh, then set `group.id`, `enable.auto.commit`, and `auto.offset.reset` deliberately.
- Re-check README-backed examples on `main` instead of copying snippets from blog posts or stale branches.
- Run the example validation and your request-reply simulations after the upgrade to catch matcher or timeout regressions early.

---

## Examples

- [README snippet compile check](src/test/scala/org/galaxio/gatling/kafka/examples/ReadmeExamplesCompileOnly.scala)
- [Scala examples](src/test/scala/org/galaxio/gatling/kafka/examples)
- [Java examples](src/test/java/org/galaxio/gatling/kafka/javaapi/examples)
- [Kotlin examples](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples)

Validate that all example simulations still construct against the current API:

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

## Contributing

```bash
# Compile the library
sbt compile

# Run the full Scala test suite in the Test scope
sbt test

# Run the Gatling simulations exercised in CI (requires Kafka/Schema Registry, for example via Docker Compose)
sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest"

# Check formatting (matches the formatting CI step)
sbt scalafmtCheckAll scalafmtSbtCheck

# Format code
sbt scalafmtAll scalafmtSbt

# Recommended local check before pushing (matches the main CI flow)
sbt clean compile "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
