package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import com.sksamuel.avro4s._
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.avro4s._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.duration.DurationInt

object ReadmeExamplesCompileOnly {

  val producerOnlyProtocol: KafkaProtocol = kafka.properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.ACKS_CONFIG              -> "1",
  )

  val requestReplyProtocolBuilder = kafka
    .producerSettings(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ProducerConfig.ACKS_CONFIG              -> "1",
    )
    .consumeSettings(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    )
    .timeout(5.seconds)

  val requestReplyProtocol: KafkaProtocol = requestReplyProtocolBuilder

  val matchByValueProtocol: KafkaProtocol = requestReplyProtocolBuilder.matchByValue

  def correlationIdFromHeader(headerName: String): KafkaProtocolMessage => Array[Byte] =
    _.headers
      .flatMap(headers => Option(headers.lastHeader(headerName)).map(_.value()))
      .getOrElse(Array.emptyByteArray)

  val matchByMessageProtocol: KafkaProtocol =
    requestReplyProtocolBuilder.matchByMessage(correlationIdFromHeader("correlation-id"))

  val produceScenario: ScenarioBuilder = scenario("readme-producer")
    .exec(
      kafka("send string")
        .topic("events")
        .send[String, String]("key", "payload"),
    )

  val requestReplyScenario: ScenarioBuilder = scenario("readme-request-reply")
    .exec(
      kafka("request reply").requestReply
        .requestTopic("requests")
        .replyTopic("replies")
        .send[String, String]("key", """{"action":"process"}""")
        .check(jsonPath("$.status").is("ok")),
    )

  final case class Ingredient(name: String, sugar: Double, fat: Double)

  val avroScenario: ScenarioBuilder = scenario("readme-avro")
    .exec(
      kafka("send avro")
        .topic("ingredients")
        .send[String, Ingredient]("key", Ingredient("Cheese", 0d, 70d)),
    )
}
