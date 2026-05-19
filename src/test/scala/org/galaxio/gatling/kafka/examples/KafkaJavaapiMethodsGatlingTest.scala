package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.protocol.Protocol
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.javaapi.KafkaDsl._
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression

import java.util.concurrent.atomic.AtomicInteger

class KafkaJavaapiMethodsGatlingTest extends Simulation {

  val c                            = new AtomicInteger(0)
  val feeder: Feeder[Int]          = Iterator.continually(Map("key" -> c.incrementAndGet()))
  val hFeeder: Feeder[Array[Byte]] = Iterator.continually(Map("headerId" -> java.util.UUID.randomUUID().toString.getBytes))

  val headers: JExpression[Headers] = s => {
    val bytes = java.util.Optional.ofNullable(s.get[Array[Byte]]("headerId")).orElse("".getBytes)
    new RecordHeaders().add("test-header", bytes)
  }

  val kafkaConfwoKey: Protocol = kafka
    .properties(
      java.util.Map.of(
        ProducerConfig.ACKS_CONFIG,
        "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )
    .protocol()

  setUp(
    scenario("Request String without key")
      .feed(hFeeder)
      .feed(feeder)
      .exec(
        kafka("Request String without headers and key")
          .topic("myTopic3")
          .send("testJavaWithoutKeyAndHeaders")
          .asScala(),
      )
      .exec(
        kafka("Request Long without headers and key")
          .topic("myTopic3")
          .send(12L)
          .asScala(),
      )
      .exec(
        kafka("Request Int Long without headers")
          .topic("myTopic3")
          .send(0, 12L)
          .asScala(),
      )
      .exec(
        kafka("Request String with headers without key")
          .topic("myTopic3")
          .send("testJavaWithHeadersWithoutKey", new RecordHeaders().add("test-header", "test_value".getBytes()))
          .asScala(),
      )
      .exec(kafka("MsgBuilders").topic("myTopic3").send("key#{key}", "val", headers).asScala())
      .inject(nothingFor(1), atOnceUsers(1))
      .protocols(kafkaConfwoKey),
  )

}
