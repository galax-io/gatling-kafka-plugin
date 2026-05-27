package org.galaxio.gatling.kafka.actions

import org.scalatest.funsuite.AnyFunSuite

class KafkaRequestFailureMessagesSpec extends AnyFunSuite {

  test("build failures keep request construction wording") {
    val message = KafkaRequestFailureMessages.buildFailure("payload expression crashed")

    assert(message == "Failed to build request: payload expression crashed")
  }

  test("send failures use broker send wording") {
    val message = KafkaRequestFailureMessages.sendFailure("timeout")

    assert(message == "Failed to send request to Kafka broker: timeout")
  }

  test("build and send failures remain distinguishable") {
    val buildMessage = KafkaRequestFailureMessages.buildFailure("boom")
    val sendMessage  = KafkaRequestFailureMessages.sendFailure("boom")

    assert(buildMessage != sendMessage)
  }
}
