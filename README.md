# Gatling Kafka Plugin

![Build](https://github.com/galax-io/gatling-kafka-plugin/workflows/Build/badge.svg) [![Maven Central](https://img.shields.io/maven-central/v/org.galaxio/gatling-kafka-plugin_2.13.svg?color=success)](https://search.maven.org/search?q=org.galaxio.gatling-kafka)  [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov.io](https://codecov.io/github/galax-io/gatling-kafka-plugin/coverage.svg?branch=master)](https://codecov.io/github/galax-io/gatling-kafka-plugin?branch=master)

# Introduction

Plugin to support Kafka in Gatling (3.11.x). Supports Avro, Protobuf (ScalaPB), and plain serialization formats.

# Usage

### Getting Started

Plugin is currently available for Scala 2.13, Java 17, Kotlin.

### Scala

```scala
libraryDependencies += "org.galaxio" %% "gatling-kafka-plugin" % <version> % Test
```

### Java

```java
gatling "org.galaxio:gatling-kafka-plugin_2.13:<version>"
```

### Kotlin

```kotlin
gatling("org.galaxio:gatling-kafka-plugin_2.13:<version>")
```

## Examples and Tests

- Scala examples: [src/test/scala/org/galaxio/gatling/kafka/examples](src/test/scala/org/galaxio/gatling/kafka/examples)
- Java examples: [src/test/java/org/galaxio/gatling/kafka/javaapi/examples](src/test/java/org/galaxio/gatling/kafka/javaapi/examples)
- Kotlin examples: [src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples)

## Quick Start (local Kafka)

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Stop:

```bash
docker compose -f docker-compose.kafka.yml down
```

## How to run tests

```bash
sbt clean compile "Test/compile"
sbt test
```

Integration tests (require Docker):

```bash
sbt "testOnly *IntegrationSpec"
```

---

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

// The implicit URL enables the serdeClass[T] implicit,
// which uses KafkaAvroSerializer/Deserializer with cached SR client
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

Import the protobuf DSL:

```scala
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.KafkaProtobufDsl._

// Generated from .proto file
import com.example.proto.MyRequest
import com.example.proto.MyResponse
```

Produce protobuf messages:

```scala
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

### How it works

The `KafkaProtobufDsl` trait provides an implicit `Serde[T]` for any `scalapb.GeneratedMessage`. Serialization uses `toByteArray` and deserialization uses the companion's `parseFrom`. No Schema Registry required for basic protobuf (raw wire format).

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
