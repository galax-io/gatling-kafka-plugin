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

  test("sendFailure with null error string falls back to unknown error") {
    val message = KafkaRequestFailureMessages.sendFailure(null: String)

    assert(message == "Failed to send request to Kafka broker: unknown error")
  }

  test("buildFailure with null error string falls back to unknown error") {
    val message = KafkaRequestFailureMessages.buildFailure(null)

    assert(message == "Failed to build request: unknown error")
  }

  test("sendFailure with exception whose getMessage returns null uses class name") {
    val exception = new RuntimeException(null: String)

    val message = KafkaRequestFailureMessages.sendFailure(exception)

    assert(message == "Failed to send request to Kafka broker: RuntimeException")
  }

  test("sendFailure with exception uses exception message when present") {
    val exception = new RuntimeException("broker unavailable")

    val message = KafkaRequestFailureMessages.sendFailure(exception)

    assert(message == "Failed to send request to Kafka broker: broker unavailable")
  }
}
