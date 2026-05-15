package org.galaxio.gatling.kafka.actions

import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

class KafkaRequestReplyActionSpec extends AnyFunSuite {

  private val protocolMessage = KafkaProtocolMessage(
    key = "request-key".getBytes(),
    value = "response-value".getBytes(),
    inputTopic = "requests",
    outputTopic = "replies",
  )

  test("requestMatchIdOrError returns request match id when matcher is valid") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val result = KafkaRequestReplyAction.requestMatchIdOrError(protocolMessage, matcher)

    assert(result.exists(_.sameElements("request-key".getBytes())))
  }

  test("requestMatchIdOrError rejects null request match id") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = null
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val result = KafkaRequestReplyAction.requestMatchIdOrError(protocolMessage, matcher)

    assert(result == Left("request matcher returned null match id"))
  }
}
