package org.galaxio.gatling.kafka.protocol

import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class KafkaProtocolBuilderNewSpec extends AnyFunSuite {

  test("matchByKafkaMatcher keeps different request and response extractors") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val protocol = KafkaProtocolBuilderNew
      .producerSettings(Map("bootstrap.servers" -> "localhost:9092"))
      .consumeSettings(Map("bootstrap.servers" -> "localhost:9092"))
      .timeout(5.seconds)
      .matchByKafkaMatcher(matcher)
      .build

    val message = KafkaProtocolMessage(
      key = "request-id".getBytes(),
      value = "response-id".getBytes(),
      inputTopic = "requests",
      outputTopic = "replies",
    )

    assert(protocol.messageMatcher.requestMatch(message).sameElements("request-id".getBytes()))
    assert(protocol.messageMatcher.responseMatch(message).sameElements("response-id".getBytes()))
  }
}
