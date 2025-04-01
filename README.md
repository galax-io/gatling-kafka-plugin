# Gatling Kafka Plugin

![Build](https://github.com/galax-io/gatling-kafka-plugin/workflows/Build/badge.svg) [![Maven Central](https://img.shields.io/maven-central/v/org.galaxio/gatling-kafka-plugin_2.13.svg?color=success)](https://search.maven.org/search?q=org.galaxio.gatling-kafka)  [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov.io](https://codecov.io/github/galax-io/gatling-kafka-plugin/coverage.svg?branch=master)](https://codecov.io/github/galax-io/gatling-kafka-plugin?branch=master)

# Introduction

Plugin to support Kafka in Gatling (3.9.x)+

| Gatling Version | Kafka Plugin Version  | Notable Changes                                                                                             |
|-----------------|-----------------------|-------------------------------------------------------------------------------------------------------------|
| 3.9.x - 3.11.x  | Releases up to 0.15.1 | May require additional akka dependency in your dependencies - e.g. com.typesafe.akka:akka-actor_2.13:2.6.20 |
| 3.13.x          | Releases after 0.15.1 | Akka dependency no longer required                                                                          |

# Usage

### Getting Started

This plugin is currently available for Scala 2.13 / Java 17 / Kotlin.
To use it, you should include it as a dependency in your gatling project with your tests.
To include:

### Scala

```scala
libraryDependencies += "org.galaxio" %% "gatling-kafka-plugin" % <version>% Test
```

### Java

Add this to your dependencies block in build.gradle:

```java
gatling "org.galaxio:gatling-kafka-plugin_2.13:<version>"
```

### Kotlin

Add this to your dependencies block in build.gradle:

```kotlin
gatling("org.galaxio:gatling-kafka-plugin_2.13:<version>")
```

## Example Scenarios

### Scala

Examples [here](src/test/scala/org/galaxio/gatling/kafka/examples)

### Java

Examples [here](src/test/java/org/galaxio/gatling/kafka/javaapi/examples)

### Kotlin

Examples [here](src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples)

## Download and create Avro schema

Avro schema is downloaded using the
plugin [sbt-schema-registry-plugin](https://github.com/galax-io/sbt-schema-registry-plugin)
and for that you need to configure schemas and url in `build.sbt` and run the command:

```bash 
sbt schemaRegistryDownload
```

To create java classes you should add use capabilities, that provide plugin [sbt-avro](https://github.com/sbt/sbt-avro).
This plugin is included in project and will do all needed for creating java classes in compile stage.
To run you should create scala object in root project directory and type `sbt run`.

### Example download avro-schema

Example [here](https://github.com/galax-io/gatling-kafka-plugin/tree/master/src/test/scala/org/galaxio/gatling/kafka/examples)

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
