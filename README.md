# Gatling Kafka Plugin

![Build](https://github.com/galax-io/gatling-kafka-plugin/workflows/Build/badge.svg) [![Maven Central](https://img.shields.io/maven-central/v/org.galaxio/gatling-kafka-plugin_2.13.svg?color=success)](https://search.maven.org/search?q=org.galaxio.gatling-kafka)  [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov.io](https://codecov.io/github/galax-io/gatling-kafka-plugin/coverage.svg?branch=master)](https://codecov.io/github/galax-io/gatling-kafka-plugin?branch=master)

# Introduction

Plugin to support Kafka in Gatling (3.11.x).

# Usage

### Getting Started

Plugin is currently available for Scala 2.13, Java 17, Kotlin.

You may include plugin as dependency in project with your tests. Write

### Scala

```scala
libraryDependencies += "org.galaxio" %% "gatling-kafka-plugin" % <version> % Test
```

### Java

Write this to your dependencies block in build.gradle:

```java
gatling "org.galaxio:gatling-kafka-plugin_2.13:<version>"
```

### Kotlin

Write this to your dependencies block in build.gradle:

```kotlin
gatling("org.galaxio:gatling-kafka-plugin_2.13:<version>")
```

## Examples and Tests

- Scala examples: [src/test/scala/org/galaxio/gatling/kafka/examples](src/test/scala/org/galaxio/gatling/kafka/examples)
- Java examples: [src/test/java/org/galaxio/gatling/kafka/javaapi/examples](src/test/java/org/galaxio/gatling/kafka/javaapi/examples)
- Kotlin examples: [src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples)
- Sender regression tests: [src/test/scala/org/galaxio/gatling/kafka/client/KafkaSenderSpec.scala](src/test/scala/org/galaxio/gatling/kafka/client/KafkaSenderSpec.scala)
- Thread load Gatling simulation: [src/test/scala/org/galaxio/gatling/kafka/examples/KafkaThreadLoadSimulation.scala](src/test/scala/org/galaxio/gatling/kafka/examples/KafkaThreadLoadSimulation.scala)

## Quick Start (local Kafka)

Run local Kafka, ZooKeeper, Schema Registry, and auto-create topics:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Stop the stack:

```bash
docker compose -f docker-compose.kafka.yml down
```

Compose file: [docker-compose.kafka.yml](docker-compose.kafka.yml)

## How to run tests

Unit and integration compilation:

```bash
sbt clean compile "Test/compile"
```

## Silent Requests (hide Gatling stats rows)

Use `silent` when a Kafka request must be executed but should not appear as a request row in Gatling statistics.

### Scala

```scala
scenario("Silent requests")
  .exec(
    kafka("Request String")
      .send[String]("foo")
      .silent,
  )
  .exec(
    kafka("Request Reply String").requestReply
      .requestTopic("myTopic1")
      .replyTopic("test.t1")
      .send[String, String]("k", """{ "m": "v" }""")
      .silent
      .check(jsonPath("$.m").is("v")),
  )
```

### Java

```java
scenario("Silent requests")
  .exec(
    kafka("Request String")
      .send("payload", new RecordHeaders(), true)
      .asScala()
  );
```

You can also use `.silent()` / `.notSilent()` on Java `RequestBuilder`.

## Multiple Topics In One Scenario

The protocol-level `.topic(...)` is only a default topic. You can override the topic on each Kafka action, so sending to different topics in a single scenario is supported.

### Scala

```scala
scenario("Multiple topics")
  .exec(
    kafka("send to topic A")
      .send("key-a", "payload-a")
      .topic("topic-a"),
  )
  .exec(
    kafka("send to topic B")
      .send("key-b", "payload-b")
      .topic("topic-b"),
  )
```

For request-reply you can also define topics per action:

```scala
scenario("Request reply with explicit topics")
  .exec(
    kafka("request reply")
      .requestReply
      .requestTopic("request-topic")
      .replyTopic("reply-topic")
      .send("key", "payload"),
  )
```

## Request-Reply Matching

When using `requestReply`, the plugin must correlate each sent message with its reply.
An extractor runs on the outgoing message to produce a byte array (the "request match ID"),
and another extractor runs on every incoming message from the reply topic.
When both extractors produce the same bytes, the reply is matched to the original request.

### Protocol-Level Matchers

Configure the default matching strategy on the protocol builder:

```scala
val proto = kafka.requestReply
  .producerSettings(...)
  .consumeSettings(...)
  .timeout(5.seconds)
  // Pick ONE:
  // (default)          — correlates by msg.key on both sides
  // .matchByValue      — correlates by msg.value on both sides
  // .matchByMessage(fn) — applies fn to both sent and received messages
  .matchByValue
```

| Method | Request extractor | Response extractor |
|--------|------------------|--------------------|
| *(default)* | `msg.key` | `msg.key` |
| `.matchByValue` | `msg.value` | `msg.value` |
| `.matchByMessage(fn)` | `fn(msg)` | `fn(msg)` |

### Action-Level Overrides

Override extractors on a single request-reply action when different actions need different matching logic:

```scala
kafka("custom match").requestReply
  .requestTopic("input")
  .replyTopic("output")
  .send[String, String]("key", "payload")
  .requestMatchBy(_.key)   // extractor for the SENT message
  .replyMatchBy(_.value)   // extractor for the RECEIVED reply
```

Action-level extractors take precedence over the protocol-level matcher for that action only.

### Matching by Header

If your consumer transforms the key and value but copies a correlation ID into a header,
neither the default key matcher nor `matchByValue` will work. Use a custom extractor that reads the header:

```scala
kafka("header match").requestReply
  .requestTopic("requests")
  .replyTopic("replies")
  .send[String, String]("key", "payload")
  .requestMatchBy(msg => msg.key) // correlation ID is the key on the request side
  .replyMatchBy { msg =>
    msg.headers
      .map(_.lastHeader("correlation-id").value())
      .getOrElse(Array.emptyByteArray)
  }
```

> **Pitfall**: `matchByValue` requires that the reply's value is byte-identical to the request's value.
> If your consumer modifies the payload in any way, matching will silently fail and requests will timeout.
> Always ensure both extractors produce the exact same bytes for a given request-reply pair.

For a full working example, see [MatchSimulation.scala](src/test/scala/org/galaxio/gatling/kafka/examples/MatchSimulation.scala).

## Consume-Only Tracking

Use `consumeFrom(...)` when the scenario should wait for and validate a Kafka message without sending a Kafka request first.
Use `consumeAny(...)` when the same read flow should consume the first available message instead of correlating by a tracking id.

This is useful for:
- Kafka consumer load testing
- mixed protocol flows such as `HTTP -> Kafka`
- workflow verification where the correlation id already exists in the Gatling `Session`

Both methods share the same runtime path: the action always reads from Kafka, and correlation becomes active only when you configure tracking data such as `matchIdForTracking(...)`, `keyForTracking(...)`, `payloadForTracking(...)`, or `headerForTracking(...)`.

`saveAs(...)` stores extracted values from the consumed Kafka message into the Gatling `Session`.
It does not publish anything to another topic by itself. If you want to forward the consumed data,
use `saveAs(...)` first and then a later `exec(kafka(...).send(...))`.

### Scala

Track by payload:

```scala
scenario("Consume only by payload")
  .exec(session => session.set("correlationId", "corr-42"))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .payloadForTracking("#{correlationId}")
      .saveAs("replyValue")(message => new String(message.value)),
  )
```

Track by key:

```scala
scenario("Consume only by key")
  .exec(session => session.set("eventKey", "order-42"))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .keyForTracking("#{eventKey}")
      .check(bodyString.exists)
      .saveAs("eventBody")(message => new String(message.value)),
  )
```

Track by header:

```scala
scenario("Consume only by header")
  .exec(session => session.set("correlationId", "corr-42"))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .headerForTracking("correlation-id", "#{correlationId}")
      .saveAs("replyValue")(message => new String(message.value)),
  )
```

For fully custom correlation you can provide the tracking bytes directly and customize how consumed messages are matched:

```scala
scenario("Consume only with custom matcher")
  .exec(session => session.set("expectedMatchId", "corr-42".getBytes))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .matchIdForTracking(session => session("expectedMatchId").validate[Array[Byte]])
      .replyMatchBy(message => message.value)
      .saveAs("rawValue")(message => message.value),
  )
```

Read the first available message without correlation data:

```scala
scenario("Consume any event")
  .exec(
    kafka("consume first available")
      .consumeAny("events")
      .check(bodyString.exists)
      .saveAs("payload")(message => new String(message.value)),
  )
```

Forward consumed data in the next step:

```scala
scenario("Consume and resend")
  .exec(session => session.set("correlationId", "corr-42"))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .headerForTracking("correlation-id", "#{correlationId}")
      .saveAs("consumedPayload")(message => new String(message.value)),
  )
  .exec(
    kafka("forward event")
      .send("#{correlationId}", "#{consumedPayload}")
      .topic("forwarded-events"),
  )
```

### Java

```java
scenario("Consume only")
  .exec(session -> session.set("matchId", "corr-42".getBytes()))
  .exec(
    kafka("consume event")
      .consumeFrom("events")
      .matchIdForTracking(byteArrayExp(session -> (byte[]) session.get("matchId")))
      .replyMatchBy(KafkaProtocolMessage::value)
      .saveAs("replyValue", message -> new String(message.value()))
  );
```

Or consume the first available message:

```java
scenario("Consume any event")
  .exec(
    kafka("consume first available")
      .consumeAny("events")
      .saveAs("payload", message -> new String(message.value()))
  );
```


## Run Gatling simulations

Run a simulation class:

```bash
sbt "testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"
```

Thread load simulation for VisualVM checks:

```bash
sbt "testOnly org.galaxio.gatling.kafka.examples.KafkaThreadLoadSimulation" \
  -Dgatling.kafka.bootstrapServers=localhost:9093 \
  -Dgatling.kafka.topic=test.t2 \
  -Dgatling.kafka.threadLoad.concurrentUsers=200 \
  -Dgatling.kafka.threadLoad.durationSeconds=300 \
  -Dgatling.kafka.threadLoad.pauseMillis=25
```

## Download and create Avro schema

Avro schema is downloaded using the plugin [sbt-schema-registry-plugin](https://github.com/galax-io/sbt-schema-registry-plugin)
and for that you need to configure schemas and url in `build.sbt` and run the command:

```bash 
sbt schemaRegistryDownload
```

To create java classes you should add use capabilities, that provide plugin [sbt-avro](https://github.com/sbt/sbt-avro).
This plugin is included in project and will do all needed for creating java classes in compile stage.
To run you should create scala object in root project directory and type `sbt run`.

### Example download avro-schema

Example [here](https://github.com/galax-io/gatling-kafka-plugin/tree/main/src/test/scala/org/galaxio/gatling/kafka/examples)

## Avro support in Request-Reply

### Scala

To use avro messages as payload in key or value, you must:

- define implicit for schema registry url:

```scala
implicit val schemaRegUrl: String = "http://localhost:9094"
```

- or define serde for your class:

```scala
val ser =
  new KafkaAvroSerializer(
    new CachedSchemaRegistryClient("schRegUrl".split(',').toList.asJava, 16),
  )

val de =
  new KafkaAvroDeserializer(
    new CachedSchemaRegistryClient("schRegUrl".split(',').toList.asJava, 16),
  )

implicit val serdeClass: Serde[MyAvroClass] = new Serde[MyAvroClass] {
  override def serializer(): Serializer[MyAvroClass] = ser.asInstanceOf[Serializer[MyAvroClass]]
  override def deserializer(): Deserializer[MyAvroClass] = de.asInstanceOf[Deserializer[MyAvroClass]]
}
```

### Java

To use avro messages as payload in key or value, you must define serde for your class:

```java
public static Serializer<MyAvroClass> ser = (Serializer) new KafkaAvroSerializer(new CachedSchemaRegistryClient(Arrays.asList("schRegUrl".split(",")), 16));
public static Deserializer<MyAvroClass> de = (Deserializer) new KafkaAvroDeserializer(new CachedSchemaRegistryClient(Arrays.asList("schRegUrl".split(",")), 16));
```

### Kotlin

To use avro messages as payload in key or value, you must define serde for your class:

```kotlin
val ser = KafkaAvroSerializer(CachedSchemaRegistryClient("schRegUrl".split(','), 16),) as Serializer<MyAvroClass>
val de = KafkaAvroDeserializer(CachedSchemaRegistryClient("schRegUrl".split(','), 16),) as Deserializer<MyAvroClass>
```

### Example usage Avro in Request-Reply

Example [scala](src/test/scala/org/galaxio/gatling/kafka/examples/AvroClassWithRequestReplySimulation.scala)

Example [java](src/test/java/org/galaxio/gatling/kafka/javaapi/examples/AvroClassWithRequestReplySimulation.java)

Example [kotlin](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples/AvroClassWithRequestReplySimulation.kt)
