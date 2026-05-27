# Gatling Kafka Plugin

[![CI](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/Build/badge.svg?branch=main)](https://github.com/galax-io/gatling-kafka-plugin/actions)
[![Maven Central](https://img.shields.io/maven-central/v/org.galaxio/gatling-kafka-plugin_2.13.svg?color=success)](https://search.maven.org/search?q=org.galaxio.gatling-kafka)
[![codecov](https://codecov.io/github/galax-io/gatling-kafka-plugin/coverage.svg?branch=main)](https://codecov.io/github/galax-io/gatling-kafka-plugin?branch=main)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Kafka protocol plugin for [Gatling](https://gatling.io/) load testing framework. Produce, request-reply, and consume messages with Avro, Protobuf (ScalaPB), and plain serialization formats.

## Table of Contents

- [Compatibility](#compatibility)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Producing Messages](#producing-messages)
- [Request-Reply](#request-reply)
- [Consume-Only Tracking](#consume-only-tracking)
- [Runtime Semantics and Troubleshooting](#runtime-semantics-and-troubleshooting)
- [Avro Support](#avro-support)
- [Protobuf Support (ScalaPB)](#protobuf-support-scalapb)
- [Architecture](#architecture)
- [Migration Guide](#migration-guide)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Compatibility

| Plugin Version | Gatling | Scala | Java |
|---|---|---|---|
| 0.x.y-latest | 3.13.x | 2.13 | 17+ |
| 0.x.y | 3.11.x | 2.13 | 17+ |

> **Branch strategy:** `main` targets Gatling 3.11.x, `latest/gatling` targets Gatling 3.13.x.

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
  val kafkaConf = kafka.topic("test-topic")
    .properties(Map("bootstrap.servers" -> "localhost:9092"))

  val scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
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
  var kafkaConf = kafka().topic("test-topic")
    .properties(Map.of("bootstrap.servers", "localhost:9092"));

  var scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
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
  val kafkaConf = kafka().topic("test-topic")
    .properties(mapOf("bootstrap.servers" to "localhost:9092"))

  val scn = scenario("Kafka Producer")
    .exec(
      kafka("send message")
        .send("key", """{"msg": "hello"}""")
    )

  init { setUp(scn.injectOpen(atOnceUsers(1)).protocols(kafkaConf)) }
}
```

## Producing Messages

### Basic Send

```scala
import org.galaxio.gatling.kafka.Predef._

scenario("Producer")
  .exec(
    kafka("send string")
      .send[String, String]("key", "payload"),
  )
```

### Partition and Timestamp Control

Target a specific partition or set an explicit timestamp on produced records:

```scala
kafka("send to partition")
  .send[String, String]("key", "payload")
  .partition(3)
  .timestamp(System.currentTimeMillis())
```

Both `.partition()` and `.timestamp()` accept Gatling `Expression` values for dynamic resolution from the session.

### Silent Requests

```scala
kafka("silent request")
  .send[String]("foo")
  .silent
```

---

## Request-Reply

```scala
kafka("request reply").requestReply
  .requestTopic("requests")
  .replyTopic("replies")
  .send[String, String]("key", """{"action": "process"}""")
  .check(jsonPath("$.status").is("ok"))
```

### Matching Strategies

| Method | Request extractor | Response extractor |
|--------|------------------|--------------------|
| *(default)* | `msg.key` | `msg.key` |
| `.matchByValue` | `msg.value` | `msg.value` |
| `.matchByMessage(fn)` | `fn(msg)` | `fn(msg)` |

Action-level overrides:

```scala
kafka("custom match").requestReply
  .requestTopic("input")
  .replyTopic("output")
  .send[String, String]("key", "payload")
  .requestMatchBy(_.key)
  .replyMatchBy(_.value)
```

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

## Runtime Semantics and Troubleshooting

### Request-reply lifecycle on this branch

This `3.11.x` backport line still uses `KafkaStreams` internally for reply tracking.
The tracker is created lazily the first time an action listens on a given `replyTopic`, then reused for later requests on the same topic.

- The request is registered for tracking only after the producer callback reports a successful send.
- Correlation state lives in memory inside the Gatling JVM.
- Timed-out entries are removed from the tracker, so late replies are ignored instead of reviving a failed request.
- Timeout checks run on a `1 second` cadence, so a request can fail slightly after the configured timeout.
- `streams.cleanUp()` is called before the tracker starts, so local Kafka Streams state is treated as disposable test state.

### Timeout and matching expectations

- The default protocol timeout is `60 seconds` when you use `.withDefaultTimeout`.
- `.timeout(...)` on the protocol or on a consume action overrides that default for reply tracking.
- The default matcher uses the Kafka record key for both request and reply correlation.
- If your service replies before the request has been registered in the tracker, that reply is treated as unmatched and is dropped.
- If you use `.matchByValue` or `.matchByMessage(...)`, make sure the extracted bytes are stable and non-null on both sides.

### Injected consume settings

For request-reply tracking, the plugin injects these defaults into `consumeSettings`:

| Setting | Value | Why |
|---|---|---|
| `application.id` | random `gatling-kafka-test-...` value | avoids collisions between test runs |
| `default.key.serde` | Kafka `ByteArraySerde` | reply matcher works on raw bytes |
| `default.value.serde` | Kafka `ByteArraySerde` | checks and deserializers receive raw bytes |

Your explicit `consumeSettings` values are merged on top of those defaults, so you can override them when needed.

### Group ids, auto-commit, and offset reset

Because this branch uses `KafkaStreams` rather than a raw `KafkaConsumer` for request-reply tracking:

- `application.id` is the setting that identifies the runtime, not `group.id`.
- The plugin does not inject request-reply defaults for `enable.auto.commit`.
- The plugin does not inject a plain `auto.offset.reset` value for request-reply tracking.
- If you were expecting `KafkaConsumer` semantics from newer branches, do not copy those settings into this `3.11.x` line unchanged.

### Troubleshooting

- Repeated timeouts usually mean the matcher does not extract the same bytes from request and reply. Start by verifying the key or custom extractor on both sides.
- A reply consumed by one request is no longer available to another request with the same correlation key, because matching removes the tracked entry.
- Very fast replies can be missed if they arrive before the send callback has registered the request for tracking.
- Reusing the same `replyTopic` across unrelated flows can mix traffic inside one tracker. Separate topics or correlation strategies are safer under load.
- If you need durable offsets or consumer-group-style replay semantics, use a dedicated consumer outside the plugin's request-reply tracker.

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

case class Ingredient(name: String, sugar: Double, fat: Double)
implicit val ingredientFormat: RecordFormat[Ingredient] = RecordFormat[Ingredient]

scenario("Avro4s")
  .exec(
    kafka("send avro")
      .send[String, Ingredient]("key", Ingredient("Cheese", 0d, 70d)),
  )
```

### Schema Registry Integration

The plugin caches Schema Registry clients per URL (thread-safe, shared across all virtual users):

```scala
implicit val schemaRegUrl: String = "http://localhost:8081"
```

### Avro in Request-Reply

```scala
implicit val schemaRegUrl: String = "http://localhost:8081"

kafka("avro request reply").requestReply
  .requestTopic("avro-requests")
  .replyTopic("avro-replies")
  .send[String, MyAvroClass]("key", myAvroInstance)
  .check(avroBody[MyAvroClass].is(expectedResponse))
```

### Avro Schema Download

Using [sbt-schema-registry-plugin](https://github.com/galax-io/sbt-schema-registry-plugin):

```bash
sbt schemaRegistryDownload
```

---

## Protobuf Support (ScalaPB)

### Setup

Add ScalaPB runtime to your test dependencies:

```scala
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.17" % Test
```

If using `sbt-protoc` for code generation from `.proto` files:

```scala
// project/plugins.sbt
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

// build.sbt
Test / PB.targets := Seq(
  scalapb.gen() -> (Test / sourceManaged).value / "scalapb",
)
```

### Usage

```scala
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.KafkaProtobufDsl._

import com.example.proto.MyRequest
import com.example.proto.MyResponse

scenario("Protobuf Producer")
  .exec(
    kafka("send protobuf")
      .send[String, MyRequest]("key", MyRequest(id = "req-1", payload = "hello")),
  )
```

### Protobuf in Request-Reply with Checks

```scala
kafka("protobuf request reply").requestReply
  .requestTopic("proto-requests")
  .replyTopic("proto-replies")
  .send[String, MyRequest]("key", MyRequest(id = "req-1", payload = "data"))
  .check(protobufBody[MyResponse].transform(_.success).is(true))
```

The `protobufBody[T]` check deserializes the response bytes using ScalaPB's `parseFrom`.

---

## Architecture

```
KafkaDsl / KafkaProtobufDsl  (entry points, implicits)
    |
KafkaRequestBuilderBase      (DSL: .send, .requestReply, .consumeFrom)
    |
    +-- KafkaProduceActionBase[K,V,P]   (shared produce logic)
    |       |-- KafkaRequestAction      (plain types, P=V)
    |       |-- KafkaAvro4sRequestAction (Avro4s, P=GenericRecord)
    |
    +-- KafkaRequestReplyAction         (produce + track reply via actor)
    +-- KafkaConsumeAction              (consume-only tracking)
    |
KafkaMessageTrackerActor               (Akka actor for correlation)
TrackersPool                            (consumer thread per topic pair)
KafkaSender / KafkaSenderPool           (producer pool)
```

---

## Migration Guide

### Reply tracking settings on the 3.11.x line

This branch still uses `KafkaStreams` for request-reply tracking, so `consumeSettings` should be written for the streams-based runtime shown below:

```scala
.consumeSettings(Map(
  "bootstrap.servers" -> "localhost:9092",
  "application.id" -> "my-test-group",
))
```

If you are looking for `KafkaConsumer`-specific settings such as `group.id`, consult the README for the branch or release line that introduced the consumer-based tracker.

---

## Examples

- [Scala examples](src/test/scala/org/galaxio/gatling/kafka/examples)
- [Java examples](src/test/java/org/galaxio/gatling/kafka/javaapi/examples)
- [Kotlin examples](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples)

## Contributing

```bash
# Build
sbt compile

# Run unit tests
sbt test

# Run integration tests (requires Docker)
sbt "testOnly *IntegrationSpec"

# Check formatting
sbt scalafmtCheckAll

# Format code
sbt scalafmtAll
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
