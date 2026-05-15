package org.galaxio.gatling.kafka.protocol

import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class KafkaProtocolBuilderSpec extends AnyFunSuite {

  test("matchByKafkaMatcher keeps different request and response extractors") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val protocol = KafkaProtocolBuilder
      .producerSettings("bootstrap.servers" -> "localhost:9092")
      .consumeSettings("bootstrap.servers" -> "localhost:9092")
      .timeout(5.seconds)
      .matchByKafkaMatcher(matcher)
      .build

    val message = KafkaProtocolMessage(
      key = "request-id".getBytes(),
      value = "response-id".getBytes(),
      producerTopic = "requests",
      consumerTopic = "replies",
    )

    assert(protocol.messageMatcher.requestMatch(message).sameElements("request-id".getBytes()))
    assert(protocol.messageMatcher.responseMatch(message).sameElements("response-id".getBytes()))
  }
}
