package org.galaxio.gatling.kafka.checks

import io.gatling.commons.validation.{Failure, Success, Validation}
import io.gatling.core.config.GatlingConfiguration
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

/** Issue #168 — a reply whose payload is absent must produce a clean failure, not an exception.
  *
  * `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer` read `msg.value.length` unguarded. `msg.value` is copied
  * verbatim from `consumerRecord.value()`, which is `null` for a tombstone and for any null-valued reply — normal traffic on a
  * compacted topic. The resulting NPE escapes `Check.check` into the tracker's `try`/`finally`, which has no `catch`, so the
  * virtual user is never continued: no success, no failure, no next request. It simply stops.
  *
  * Two of the five preparers in the same object already get this right, wrapping the identical work in `safely(...)`, which is
  * the strongest evidence the other three are an oversight rather than a decision.
  *
  * Absent is not empty. An empty payload is a value and keeps its existing behaviour — `bodyString.is("")` must pass on an
  * empty reply and fail on a tombstone, because "the service sent nothing" and "the service sent an empty string" are different
  * findings.
  */
class KafkaMessagePreparerSpec extends munit.FunSuite {

  private val config      = GatlingConfiguration.loadForTest()
  private val jsonParsers = io.gatling.core.Predef.defaultJsonParsers

  private def message(value: Array[Byte]): KafkaProtocolMessage =
    KafkaProtocolMessage(
      key = "k".getBytes,
      value = value,
      producerTopic = "in",
      consumerTopic = "out",
    )

  private def failureOf[T](v: Validation[T]): String = v match {
    case Failure(message) => message
    case Success(value)   => fail(s"expected a failure, got a success: $value")
  }

  private def succeeds[T](v: Validation[T]): T = v match {
    case Success(value)   => value
    case Failure(message) => fail(s"expected a success, got a failure: $message")
  }

  // --- absent payload: every preparer must fail, and say why -------------------------------------

  test("stringBodyPreparer fails on an absent payload instead of throwing") {
    val message1 = failureOf(KafkaMessagePreparer.stringBodyPreparer(config)(message(null)))
    assert(message1.toLowerCase.contains("no payload"), s"unexpected message: $message1")
  }

  test("bytesBodyPreparer fails on an absent payload instead of throwing") {
    val message1 = failureOf(KafkaMessagePreparer.bytesBodyPreparer(message(null)))
    assert(message1.toLowerCase.contains("no payload"), s"unexpected message: $message1")
  }

  test("jsonPathPreparer fails on an absent payload instead of throwing") {
    val message1 = failureOf(KafkaMessagePreparer.jsonPathPreparer(jsonParsers, config)(message(null)))
    assert(message1.toLowerCase.contains("no payload"), s"unexpected message: $message1")
  }

  test("xmlPreparer fails on an absent payload instead of throwing") {
    // Already guarded by `safely`, so this passes before the fix too. It is here to pin that all five
    // agree: a reply must not succeed under one check type and strand the user under another.
    val message1 = failureOf(KafkaMessagePreparer.xmlPreparer(config)(message(null)))
    assert(message1.nonEmpty)
  }

  // --- empty payload: unchanged, and must stay distinct from absent -------------------------------

  test("an empty payload is a value, not an absent one") {
    assertEquals(succeeds(KafkaMessagePreparer.stringBodyPreparer(config)(message(Array.emptyByteArray))), "")
    assertEquals(
      succeeds(KafkaMessagePreparer.bytesBodyPreparer(message(Array.emptyByteArray))).length,
      0,
    )
  }

  test("a present payload is still parsed") {
    assertEquals(succeeds(KafkaMessagePreparer.stringBodyPreparer(config)(message("hello".getBytes))), "hello")
    assertEquals(
      new String(succeeds(KafkaMessagePreparer.bytesBodyPreparer(message("hello".getBytes)))),
      "hello",
    )
    assertEquals(
      succeeds(KafkaMessagePreparer.jsonPathPreparer(jsonParsers, config)(message("""{"m":"v"}""".getBytes)))
        .get("m")
        .asText(),
      "v",
    )
  }
}
