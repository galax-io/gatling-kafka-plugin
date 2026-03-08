package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.{Callback, MockProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}

class KafkaSenderSpec extends AnyWordSpec with Matchers {

  "KafkaSender" should {
    "send asynchronously without waiting for broker ack" in {
      val producer = new MockProducer[Array[Byte], Array[Byte]](false, new ByteArraySerializer(), new ByteArraySerializer())
      val sender   = KafkaSender.fromProducer(producer)
      val message  = KafkaProtocolMessage("k".getBytes, "v".getBytes, "in-topic", "out-topic")

      @volatile var successCalled = false
      @volatile var failureCalled = false
      val successLatch            = new CountDownLatch(1)
      val failureLatch            = new CountDownLatch(1)

      sender.send(message)(
        _ => {
          successCalled = true
          successLatch.countDown()
        },
        _ => {
          failureCalled = true
          failureLatch.countDown()
        },
      )

      successCalled shouldBe false
      failureCalled shouldBe false
      successLatch.await(100, TimeUnit.MILLISECONDS) shouldBe false
      failureLatch.await(100, TimeUnit.MILLISECONDS) shouldBe false

      producer.completeNext() shouldBe true
      successLatch.await(1, TimeUnit.SECONDS) shouldBe true

      successCalled shouldBe true
      failureCalled shouldBe false
    }

    "route broker errors to failure callback" in {
      val expected = new RuntimeException("boom")

      val handler = new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
          case "send" if args != null && args.length == 2 =>
            val callback = args(1).asInstanceOf[Callback]
            callback.onCompletion(null, expected)
            val failed   = new CompletableFuture[RecordMetadata]()
            failed.completeExceptionally(expected)
            failed
          case "close"                                    => null
          case _                                          =>
            throw new UnsupportedOperationException(s"Method ${method.getName} is not supported in this test proxy")
        }
      }

      val producer = Proxy
        .newProxyInstance(
          classOf[Producer[Array[Byte], Array[Byte]]].getClassLoader,
          Array(classOf[Producer[Array[Byte], Array[Byte]]]),
          handler,
        )
        .asInstanceOf[Producer[Array[Byte], Array[Byte]]]

      val sender  = KafkaSender.fromProducer(producer)
      val message = KafkaProtocolMessage("k".getBytes, "v".getBytes, "in-topic", "out-topic")

      @volatile var successCalled      = false
      @volatile var failure: Throwable = null
      val successLatch                 = new CountDownLatch(1)
      val failureLatch                 = new CountDownLatch(1)

      sender.send(message)(
        _ => {
          successCalled = true
          successLatch.countDown()
        },
        e => {
          failure = e
          failureLatch.countDown()
        },
      )

      failureLatch.await(1, TimeUnit.SECONDS) shouldBe true
      successLatch.await(100, TimeUnit.MILLISECONDS) shouldBe false

      successCalled shouldBe false
      failure shouldBe expected
    }
  }
}
