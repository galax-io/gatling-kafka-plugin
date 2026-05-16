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
