# Gatling Kafka Plugin

[![CI](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/ci.yml)
[![Release](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/release.yml/badge.svg)](https://github.com/galax-io/gatling-kafka-plugin/actions/workflows/release.yml)
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
- [Releasing](#releasing)
- [License](#license)

## Compatibility

| Branch / Line | Gatling | Scala | Java | Kafka client |
|---|---|---|---|---|
| `main` / `1.3.x` | 3.13.5 | 2.13.18 | 17+ | Apache 3.9.x |
| `1.1.x` – `1.2.x` | 3.13.5 | 2.13.18 | 17+ | Confluent 7.9.x-ce |
| `1.0.x` | 3.13.5 | 2.13.16 | 17+ | Confluent 7.9.x-ccs |
| 0.22.x | 3.13.x | 2.13 | 17+ | Confluent 7.x |
| 0.21.x | 3.12.x | 2.13 | 17+ | Confluent 7.x |
| 0.20.3 | 3.11.5 | 2.13 | 17+ | Confluent 7.x |

> **Kafka client:** from `1.3.0` the plugin depends on the Apache release of `kafka-clients`
> (`org.apache.kafka:kafka-clients:3.9.x`) rather than Confluent's `-ce` rebuild of the same upstream
> code. Confluent Platform 7.9.x is built from Apache Kafka 3.9.x, so this is the same code under a
> different version scheme — but the Confluent rebuild is published only to `packages.confluent.io`,
> which made the plugin unresolvable for anyone building against Maven Central. Broker compatibility is
> unchanged.

> **Version guidance:** if you are on Gatling `3.11.5`, use plugin `0.20.3`. The `1.0.x` / `main` line targets Gatling `3.13.x`.
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

Everything above resolves from Maven Central. No additional repository is required for plain
serialization — which covers producing, request-reply, checks, and consume-only tracking.

### Optional: Avro via Confluent Schema Registry

Schema-Registry-backed Avro needs two artifacts that Confluent publishes only to its own repository —
they are not on Maven Central, so the plugin declares them as `provided` and you add them yourself.
This mirrors how `avro4s` has always worked here. **Skip this section entirely if you do not use
Schema Registry**; nothing else in the plugin needs it.

```scala
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer"    % "7.9.9" % Test,
  "io.confluent" % "kafka-streams-avro-serde" % "7.9.9" % Test,
).map(_.exclude("org.apache.kafka", "kafka-clients"))
```

```kotlin
repositories {
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    gatling("io.confluent:kafka-avro-serializer:7.9.9") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    gatling("io.confluent:kafka-streams-avro-serde:7.9.9") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
}
```

```xml
<repositories>
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.9.9</version>
    <scope>test</scope>
    <exclusions>
      <exclusion><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId></exclusion>
    </exclusions>
  </dependency>
  <dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-streams-avro-serde</artifactId>
    <version>7.9.9</version>
    <scope>test</scope>
    <exclusions>
      <exclusion><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId></exclusion>
    </exclusions>
  </dependency>
</dependencies>
```

**The `kafka-clients` exclusion is not optional — keep it.** These artifacts pull
`io.confluent:kafka-schema-registry-client`, which depends on Confluent's own rebuild of the Kafka
client (`kafka-clients:7.9.x-ccs`). sbt and Gradle both resolve conflicts by taking the highest
version, so that rebuild wins over the Apache `3.9.x` this plugin declares and your load test silently
runs a different client from the one the plugin is built and tested against. Excluding it leaves the
plugin's own Apache client in place. Maven resolves nearest-wins and is not affected, but the exclusion
is harmless there and keeps the three snippets equivalent.

Your simulation code needs no change: `org.galaxio.gatling.kafka.Predef._` exposes the Avro serdes
exactly as before. See [Schema Registry Integration](#schema-registry-integration) for usage.

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
    .orNull
```

> **Return `null`, not `Array.emptyByteArray`, when the field is missing.** An empty array is a *value*: every request missing
> the header would produce the same correlation id, they would all share one slot, and replies would be matched to the wrong
> virtual user. Returning `null` makes the plugin fail those requests immediately with a message naming the cause.

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

> **Requires two extra dependencies.** From `1.3.0` the Confluent Schema Registry artifacts are
> `provided` rather than inherited, because they are not published to Maven Central. Add them and the
> Confluent resolver as shown in
> [Installation → Optional: Avro via Confluent Schema Registry](#optional-avro-via-confluent-schema-registry)
> before using anything in this section. Without them the code below still compiles, and fails at run
> time with `NoClassDefFoundError: io/confluent/kafka/streams/serdes/avro/GenericAvroSerde`.

For Schema Registry-backed Avro classes, provide an implicit `schemaRegUrl` or your own Kafka `Serde[T]`:

```scala
implicit val schemaRegUrl: String = "http://localhost:8081"
```

The same applies to `avroBody` checks and to the Java facade's `avro(...)` entry points, which are
backed by the same Confluent serdes.

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
| `0.20.3` | `1.0.x` | Move from Gatling `3.11.5` to `3.13.x`, update request-reply consumer settings, re-check examples against current README. |
| `0.21.x` | `1.0.x` | Stay on Gatling `3.13.x`, review request-reply defaults and DSL surface. |
| `0.20.x` or older | `1.0.x` | Treat as full doc refresh. Older consume-only or per-action matcher APIs are not present. |
| `1.0.x` – `1.2.x` | `1.3.x` | Build-file only. Plain users: no change. Schema Registry Avro users: declare two artifacts and the Confluent resolver — see below. |
| `1.3.x` | `2.0.0` | Source-breaking, but only for API that could not work. Most suites need no change — see below. |

### `1.3.x` → `2.0.0` — removals

`2.0.0` removes published API. Every removal below is something that either could not run, never
carried a value, or had no caller — nothing that worked has been taken away. **If your simulations
use `kafka("name").topic(...).send(...)` and
`kafka("name").requestReply.requestTopic(...).replyTopic(...).send(...)`, you need no source change
at all.**

#### `send(...)` without a topic is gone

The `send(...)` overloads that could be called directly on `kafka("name")` — without `.topic(...)`
or `.requestReply...` first — have been removed from both the Scala DSL and the `javaapi` facade.

They never worked. Every action they built carried no producer topic and failed at send time with
`Kafka producer topic is not defined`; the Java `sendWithClass(payload, class, headers)` overload
threw `IllegalArgumentException` while the scenario was still being constructed. If you have one of
these in a suite, it has been reporting failures rather than sending.

```scala
// before — compiles, fails at run time
kafka("request").send[String, String]("key", "payload")

// after — name the topic first
kafka("request").topic("my-topic").send[String, String]("key", "payload")
```

#### `kafka-streams-scala` is no longer inherited

`sessionWindowedSerde` and `consumedFromSerde`, deprecated in `1.3.0`, are removed — and with them
the `org.apache.kafka:kafka-streams-scala` dependency your build used to receive transitively. The
plugin never built a Streams topology, so nothing in it used them.

If you genuinely build Streams topologies in your harness, declare the artifact yourself:

```scala
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.9.2" % Test
```

The inherited dependency set is now `scala-library`, `kafka-clients` and `avro` — three coordinates,
each used by plugin code.

#### `KafkaProtocolMessage.responseCode` is gone

Nothing ever set it: every message carried `None` from the day it was added. **Your reports do not
change.** The failure type shown for a failed request comes from a different source — the exception's
own class name, set by the request-reply action and the timeout path — and is untouched.

If you read the field, drop the read. If you matched on it, it was always `None`.

#### `KafkaCheckType.ResponseCode` is gone

Use `KafkaCheckType.Simple`. Nothing could produce a check carrying `ResponseCode`, and its
materialization was identical to `Simple`'s, so behaviour is unchanged.

#### Java produce-only request names now resolve Gatling EL

`kafka("name").topic(...)` previously passed the request name through as a literal, while
`kafka("name").requestReply()...` resolved it as a Gatling expression. The produce-only path now
matches request-reply.

For almost every suite this changes nothing — a plain name like `"BasicRequest"` resolves to itself.
It matters only if your request name contains `#{...}`: it used to appear verbatim in reports and now
resolves per virtual user, and a name referring to a session attribute that is not set will fail the
request instead of reporting the literal.

```java
// resolves per user now; previously reported literally as "order-#{orderId}"
kafka("order-#{orderId}").topic("orders").send(key, payload);
```

If you were relying on the literal, escape it (`\#{orderId}`) or rename the request.

#### `timeout` / `withDefaultTimeout` on the producer-settings step are gone

The reply timeout belongs to the consume step — a produce-only protocol never waits for a reply.
Both methods remain on `consumeSettings(...)`:

```scala
kafka.producerSettings(...).consumeSettings(...).timeout(10.seconds)   // unchanged
```

For a produce-only protocol use `kafka.properties(...)`.

### `1.2.x` → `1.3.x` — Confluent artifacts are no longer inherited

**If you use plain serialization, avro4s, or anything other than Confluent Schema Registry: nothing
to do.** Bump the version and carry on. You may also drop the Confluent resolver from your build if
you added one — it is no longer needed.

**Why this changed.** Up to `1.2.x` the plugin declared four dependencies that are published only to
`packages.confluent.io`, while its released POM carries no repository list. A consumer building against
Maven Central alone could not resolve the plugin at all. Two of the four (the Kafka client and Kafka
Streams Scala) were Confluent rebuilds of Apache code and now use the Apache coordinates. The other two
are genuinely Confluent-only and have become optional.

**If you use Schema-Registry-backed Avro**, your build previously received these transitively. Declare
them yourself, exactly as you already declare `avro4s`:

```scala
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer"    % "7.9.9" % Test,
  "io.confluent" % "kafka-streams-avro-serde" % "7.9.9" % Test,
).map(_.exclude("org.apache.kafka", "kafka-clients"))
```

Keep the `kafka-clients` exclusion — without it these artifacts pull Confluent's own rebuild of the
Kafka client, which outranks the Apache one this plugin declares under sbt's and Gradle's
highest-version-wins resolution. See
[Installation](#optional-avro-via-confluent-schema-registry) for the Gradle and Maven forms and the
full explanation.

**No source change is required, in any scenario.** Imports, implicits, and every Scala and Java entry
point are unchanged — `Predef` still supplies the Avro serdes.

**How you find out if you forget them.** Not at build time: `provided` dependencies are simply absent
from your classpath, so resolution and compilation both succeed. The serdes construct their Confluent
delegate on first use, so the first Avro send or check fails with
`NoClassDefFoundError: io/confluent/kafka/streams/serdes/avro/GenericAvroSerde` — in the middle of a
run. If your suite uses Schema Registry Avro, add the dependencies before you upgrade rather than
finding out from a load test.

**Also in this release**, `sessionWindowedSerde` and `consumedFromSerde` are deprecated. They are Kafka
Streams helpers that this plugin never used; they will be removed in `2.0.0` along with the
`kafka-streams-scala` dependency. If you genuinely build Streams topologies in your harness, depend on
`org.apache.kafka:kafka-streams-scala_2.13` directly.

### Upgrading to `1.2.0`

No changes to the DSL, the `javaapi` facade or protocol settings — nothing you have written stops compiling. Three
behavioural changes, and two of them can turn a passing scenario red, so read the sections below before upgrading.

#### A request-reply with no key is now failed instead of mismatched

A request with no key produced an empty correlation id — and so did every other keyless request. They shared a single slot in
the correlation table, so a reply resolved whichever request happened to occupy it: one virtual user was credited with another
user's answer while the real owner timed out. Nothing in the report distinguished that from a genuine result.

Under the default `matchByKey` there is nothing to correlate a keyless reply on, so such a request is now **reported as a
failure at issue time and is not published**. The failure names the matcher and the remedy.

**If a request-reply scenario of yours has no key, it will now go red.** Those runs were reporting incorrect results before;
the change surfaces that rather than causing it. Two ways forward, depending on what the request actually correlates on:

```scala
// Give each request a key to correlate on
kafka("req").requestReply
  .requestTopic("in").replyTopic("out")
  .send[String, String]("#{correlationId}", "payload")
```

```scala
// Or correlate on something the request already carries
val protocol = kafka
  .producerSettings(...)
  .consumeSettings(...)
  .matchByValue                       // the payload itself
// .matchByMessage(msg => ...)        // or a header / any extracted field
```

Request-reply that already sets a key, or that uses `matchByValue` / `matchByMessage`, is unaffected.

#### A reply with no payload now fails the request instead of losing the virtual user

A reply can arrive with no payload at all — a tombstone on a compacted topic, or an acknowledgement carrying no body. Applying
a content check to one (`bodyString`, `substring`, `bodyBytes`, `jsonPath`, `jmesPath`) used to throw inside the reply-handling
path, which had nothing to catch it. The virtual user was **never continued**: no success, no failure, no next request. It
simply stopped, and the run's user count silently diverged from the load the profile was applying.

Such a check now reports the request as a failure naming the absent payload, and the virtual user carries on.

- **Expect new KOs on any target that answers with tombstones.** Those requests were previously unreported — the run looked
  cleaner than it was, while quietly shedding load.
- **Expect the run to finish with the number of users it started with.** If your achieved throughput used to drift below the
  configured rate for no visible reason, this is a candidate cause.
- An **empty** payload is unchanged: it is a value, not an absence. `bodyString.is("")` still passes on an empty reply and now
  fails on a tombstone — "the service sent nothing" and "the service sent an empty string" are different findings.

Independently of the checks above, **no check can strand a virtual user any more**: one that throws for any reason is reported
as a failure and the user continues.

#### A message with no key is now published with no key

The plugin was substituting an empty byte array for an absent key, which is not the same thing: an empty key is a *present*
key. Kafka hashes it, and `murmur2` of an empty input is a constant — so **every keyless message landed on the same partition
for the whole run**, no matter how long the run was or how many partitions the topic had. This applied to fire-and-forget
sends as well as request-reply.

Keyless messages now reach the broker with a genuinely absent key, so Kafka applies its normal keyless partitioning instead of
hashing a constant.

- **Expect throughput, partition-lag and consumer-group numbers for keyless scenarios to move**, in either direction. The
  earlier figures described a single-partition workload, which is not what those scenarios were written to measure.
- Exactly how records are spread is Kafka's decision and depends on your broker and client version — current clients batch
  keyless records stickily rather than round-robin, so a short run may still concentrate them. What changed here is that
  spreading becomes possible at all.
- Messages that carry a key are unaffected: placement is still `hash(key) % partitions`, so per-key ordering guarantees hold.
- Immediate rejections (the section above) are reported with a near-zero response time, because that is how long they take.
  If you assert on `global.responseTime` percentiles, note that a run rejecting every request will *lower* them; assert on
  `failedRequests`/`successfulRequests` to catch that case.

> ### ⚠️ Keyless sends to a log-compacted topic now fail
>
> A compacted topic (`cleanup.policy=compact`) requires every record to have a key, and Kafka treats an **empty** key as
> present but a **null** key as absent. The old empty-array substitution therefore slipped past that check; a genuinely absent
> key does not.
>
> A scenario that publishes keyless records — request-reply or fire-and-forget — to a compacted topic goes from passing to
> **every request failing**, with `InvalidRecordException: Compacted topic cannot accept message without key`.
>
> This is the broker enforcing a rule the plugin was previously hiding: those records were never valid on that topic. Give the
> send a key:
>
> ```scala
> kafka("req").topic("compacted-topic").send[String, String]("#{entityId}", "payload")
> ```

### Upgrading to `1.1.0`

No changes to the DSL, the `javaapi` facade, protocol settings or wire formats. One behavioural change worth knowing about, in request-reply only.

#### A request whose reply channel cannot be established is no longer published

Request-reply now registers the pending request **before** handing the record to the producer, so that a reply cannot arrive
before the plugin is watching for it. Previously the request was sent first and the reply channel acquired afterwards, which
meant a reply from a fast responder could be received and silently discarded, and the request then failed on its reply timeout
as though nothing had answered.

The consequence: when acquiring the reply channel fails — for example the reply topic is never assigned within the configured
timeout — the request is now reported as a failure **without** being published. Before, it was published first and then
reported as a failure.

- Reported results are unchanged: the same KO, the same error message, and the same response-time span.
- What changes is that your system under test no longer receives a request whose reply the plugin could never have matched.
- If a simulation depended on that request reaching the broker despite the failure, it will now see one fewer record on that
  path.

#### Reported request-reply times now include the produce leg

A request-reply is now measured from the moment the record is handed to the producer. Previously it was measured from the
broker's acknowledgement of that record, which excluded the produce round trip from every reported time.

**Expect reported times to grow by one produce acknowledgement** — typically a few milliseconds against a local broker, more
with `acks=all` or a loaded one. Nothing about the requests changed; only where the clock starts.

This is the interval the virtual user actually waits for, and it is what every other Gatling protocol reports. If you compare
percentiles across this upgrade, compare them knowing the earlier numbers omitted a leg.

Channel setup is still never included: the clock starts after the reply channel exists, so a first request on a new reply topic
is not charged for its subscription and rebalance.

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

Enable the shared git hook once per clone — `pre-commit` runs scalafmt and re-stages the files
you touched, so CI's formatting gate never trips on you:

```bash
./scripts/install-hooks.sh
```

Bypass it with `SKIP_SCALAFMT=1 git commit …` (or `git commit --no-verify`) when needed.

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/) — release
notes are generated from them.

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

## Releasing

Releases are manual and tag-driven. Pushing a `vX.Y.Z` tag that is reachable from `main` (or a
`release/*` branch) runs [`release.yml`](.github/workflows/release.yml): it compiles, tests,
publishes to Sonatype via `sbt-ci-release` (version derived from the tag by dynver), and opens a
GitHub Release with notes rendered by [git-cliff](https://git-cliff.org) from
[`cliff.toml`](cliff.toml).

```bash
git checkout main && git pull
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

Nothing publishes from a branch push — [`ci.yml`](.github/workflows/ci.yml) only lints, compiles,
and tests. A tag that is not on `main`/`release/*` is rejected by the workflow. Published
coordinates are immutable: to fix a bad release, ship the next patch version rather than moving
the tag.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
