package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.concurrent.TimeLimits
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class KafkaSenderSpec extends AnyWordSpec with Matchers with TimeLimits {

  implicit private val ec: ExecutionContext = ExecutionContext.global

  "KafkaSender" should {
    "send asynchronously without waiting for broker ack" in {
      val producer = new MockProducer[Array[Byte], Array[Byte]](false, new ByteArraySerializer(), new ByteArraySerializer())
      val sender   = KafkaSender.fromProducer(producer)
      val message  = KafkaProtocolMessage("k".getBytes, "v".getBytes, "in-topic", "out-topic")

      @volatile var successCalled = false
      @volatile var failureCalled = false

      failAfter(Span(1, Seconds)) {
        sender.send(message)(_ => successCalled = true, _ => failureCalled = true)
      }

      successCalled shouldBe false
      failureCalled shouldBe false

      producer.completeNext() shouldBe true

      failAfter(Span(200, Millis)) {
        while (!successCalled) {
          Thread.`yield`()
        }
      }

      successCalled shouldBe true
      failureCalled shouldBe false
    }

    "route broker errors to failure callback" in {
      val producer = new MockProducer[Array[Byte], Array[Byte]](false, new ByteArraySerializer(), new ByteArraySerializer())
      val sender   = KafkaSender.fromProducer(producer)
      val message  = KafkaProtocolMessage("k".getBytes, "v".getBytes, "in-topic", "out-topic")
      val expected = new RuntimeException("boom")

      @volatile var successCalled = false
      @volatile var failure: Throwable = null

      producer.errorNext(expected)
      sender.send(message)(_ => successCalled = true, e => failure = e)
      producer.completeNext() shouldBe true

      failAfter(Span(200, Millis)) {
        while (failure == null) {
          Thread.`yield`()
        }
      }

      successCalled shouldBe false
      failure shouldBe expected
    }
  }
}
