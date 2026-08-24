package org.galaxio.gatling.kafka.actions

import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMessageMatcher, KafkaValueMatcher}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeoutException

class KafkaRequestFailureMessagesSpec extends AnyFunSuite {

  test("send failures use broker send wording") {
    val message = KafkaRequestFailureMessages.sendFailure("timeout")

    assert(message == "Failed to send request to Kafka broker: timeout")
  }

  test("sendFailure with null error string falls back to unknown error") {
    val message = KafkaRequestFailureMessages.sendFailure(null: String)

    assert(message == "Failed to send request to Kafka broker: unknown error")
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

  test("a failure is reported by its kind as well as by its text") {
    // The kind is the half a reader groups failures by, and it has nowhere else to go: Gatling takes a
    // response code beside the message and discards it before writing any OSS report (issue #254).
    val message = KafkaRequestFailureMessages.failureCause(new TimeoutException("Expiring 1 record(s)"))

    assert(message == "TimeoutException: Expiring 1 record(s)")
  }

  test("failureCause with no message reports the kind alone rather than a trailing null") {
    assert(KafkaRequestFailureMessages.failureCause(new RuntimeException(null: String)) == "RuntimeException")
    assert(KafkaRequestFailureMessages.failureCause(new RuntimeException("   ")) == "RuntimeException")
  }

  test("failureCause names an anonymous exception class by its full name") {
    // `getSimpleName` is empty for an anonymous subclass, which would report a message opening with ": ".
    val message = KafkaRequestFailureMessages.failureCause(new RuntimeException("boom") {})

    assert(message.endsWith(": boom"), s"unexpected message: $message")
    assert(message.contains("KafkaRequestFailureMessagesSpec"), s"unexpected message: $message")
  }

  test("missing correlation id names the matcher that could not correlate") {
    val message = KafkaRequestFailureMessages
      .missingCorrelationId("KafkaKeyMatcher", KafkaRequestFailureMessages.remedyFor(KafkaKeyMatcher))

    assert(message.contains("KafkaKeyMatcher"))
    assert(message.contains("supplies no value"))
  }

  test("the remedy fits the matcher that failed, rather than always advising a key") {
    // Parameterising the diagnosis but hardcoding the fix told a matchByValue user to "correlate with
    // matchByValue", which they already were.
    assert(KafkaRequestFailureMessages.remedyFor(KafkaKeyMatcher).contains("Set a key"))

    val byValue = KafkaRequestFailureMessages.remedyFor(KafkaValueMatcher)
    assert(byValue.contains("payload cannot be null"))
    assert(!byValue.contains("Set a key"))

    val byMessage = KafkaRequestFailureMessages.remedyFor(KafkaMessageMatcher(_.key))
    assert(byMessage.contains("extractor"))
    // The trap this whole change is about: an empty array is not "no id", it is one shared id.
    assert(byMessage.contains("empty array"))
  }

  test("missing correlation id is distinguishable from a reused match id") {
    // Two different defects with two different remedies: no identity at all versus an identity that is
    // not unique. Reporting the second wording for the first is what issue #167 did.
    val message = KafkaRequestFailureMessages
      .missingCorrelationId("KafkaKeyMatcher", KafkaRequestFailureMessages.remedyFor(KafkaKeyMatcher))

    assert(!message.contains("reused"))
  }
}
