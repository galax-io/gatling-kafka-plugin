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

### From KafkaStreams to KafkaConsumer

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
