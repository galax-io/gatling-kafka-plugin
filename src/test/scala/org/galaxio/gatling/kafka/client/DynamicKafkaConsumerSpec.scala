package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}

class DynamicKafkaConsumerSpec extends munit.FunSuite {

  private def newConsumer(): DynamicKafkaConsumer[Array[Byte], Array[Byte]] =
    DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      _ => (),
    )

  private def subscriptionQueue(
      consumer: DynamicKafkaConsumer[_, _],
  ): java.util.Queue[(String, CompletableFuture[Void])] = {
    val field = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("topicsQueue")
    field.setAccessible(true)
    field.get(consumer).asInstanceOf[java.util.Queue[(String, CompletableFuture[Void])]]
  }

  private def failureRef(consumer: DynamicKafkaConsumer[_, _]): AtomicReference[Exception] = {
    val field = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("consumerFailure")
    field.setAccessible(true)
    field.get(consumer).asInstanceOf[AtomicReference[Exception]]
  }

  private def markConsumerFailed(consumer: DynamicKafkaConsumer[_, _], exception: Exception): Unit = {
    val method = classOf[DynamicKafkaConsumer[_, _]].getDeclaredMethod("markConsumerFailed", classOf[Exception])
    method.setAccessible(true)
    method.invoke(consumer, exception)
  }

  private def causeOf(future: CompletableFuture[Void]): Throwable =
    intercept[ExecutionException](future.get(1, TimeUnit.SECONDS)).getCause

  test("removeTopicSubscription queues topic for removal") {
    val consumer = newConsumer()
    try {
      consumer.removeTopicSubscription("topic-1")
      consumer.removeTopicSubscription("topic-2")

      val field = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("topicsToRemove")
      field.setAccessible(true)
      val queue = field.get(consumer).asInstanceOf[java.util.Queue[String]]

      assertEquals(queue.size(), 2)
      assertEquals(queue.poll(), "topic-1")
      assertEquals(queue.poll(), "topic-2")
    } finally {
      consumer.close()
    }
  }

  // No consumer thread runs in this suite, so nothing here can complete a readiness: these tests cover
  // queueing and the fail-fast paths only. Readiness actually resolving from an assignment needs a
  // broker and is covered by TrackerAcquisitionIsolationSpec.
  test("requestTopicSubscription queues the topic and returns without blocking") {
    val consumer = newConsumer()
    try {
      val queue       = subscriptionQueue(consumer)
      val initialSize = queue.size()

      val readiness = consumer.requestTopicSubscription("new-topic")

      assertEquals(queue.size(), initialSize + 1)
      assertEquals(queue.peek()._1, "new-topic")
      intercept[TimeoutException](readiness.get(50, TimeUnit.MILLISECONDS))
    } finally {
      consumer.close()
    }
  }

  test("requestTopicSubscription returns a failed future after consumer failure") {
    val consumer = newConsumer()
    try {
      failureRef(consumer).set(new RuntimeException("boom"))

      val readiness = consumer.requestTopicSubscription("new-topic")

      assert(readiness.isCompletedExceptionally, "expected an already-failed readiness future")
      assertEquals(subscriptionQueue(consumer).size(), 0, "failed request must not be queued")
      assertEquals(causeOf(readiness).getCause.getMessage, "boom")
    } finally {
      consumer.close()
    }
  }

  test("consumer failure fails readiness futures queued before it") {
    val consumer = newConsumer()
    try {
      val readiness = consumer.requestTopicSubscription("new-topic")
      assert(!readiness.isDone)

      markConsumerFailed(consumer, new RuntimeException("boom"))

      assert(readiness.isCompletedExceptionally, "queued readiness must fail when the consumer fails")
      assertEquals(causeOf(readiness).getCause.getMessage, "boom")
    } finally {
      consumer.close()
    }
  }

  test("close fails readiness futures that are still pending") {
    val consumer  = newConsumer()
    val readiness = consumer.requestTopicSubscription("new-topic")
    assert(!readiness.isDone)

    consumer.close()

    assert(readiness.isCompletedExceptionally, "pending readiness must not survive close")
    assert(
      causeOf(readiness).getMessage.contains("closed"),
      s"unexpected failure message: ${causeOf(readiness).getMessage}",
    )
  }
}
