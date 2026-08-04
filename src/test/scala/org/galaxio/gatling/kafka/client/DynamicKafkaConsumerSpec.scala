package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, CopyOnWriteArrayList, ExecutionException, TimeUnit, TimeoutException}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

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

  // Issue #143. The initialization wait starts when the pool is built, before any virtual user runs, so
  // a simulation that ramps or warms up before its first request-reply reaches its expiry with nothing
  // requested. The expiry used to fall through into a poll on a consumer that had never subscribed,
  // which threw and latched a failure that poisoned every tracker for the rest of the run.
  private def newConsumerWithInitWait(
      onFailure: Exception => Unit,
      initializationTimeout: FiniteDuration,
  ): DynamicKafkaConsumer[Array[Byte], Array[Byte]] =
    DynamicKafkaConsumer.withInitializationTimeout[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      onFailure,
      initializationTimeout,
    )

  private def runningConsumer(
      consumer: DynamicKafkaConsumer[Array[Byte], Array[Byte]],
  )(body: => Unit): Unit = {
    val thread = new Thread(consumer, "init-wait-expiry")
    thread.setDaemon(true)
    thread.start()
    try body
    finally {
      consumer.close()
      thread.join(10_000)
      assert(!thread.isAlive, "the consumer thread must exit after close")
    }
  }

  test("the initialization wait expiring with no topic requested neither throws nor fails the consumer") {
    val failures = new CopyOnWriteArrayList[Exception]()
    val consumer = newConsumerWithInitWait(failures.add(_), 100.millis)

    runningConsumer(consumer) {
      // An order of magnitude past the wait, so the loop has taken many turns with nothing subscribed.
      Thread.sleep(1_000)

      assertEquals(
        failures.asScala.map(_.getMessage).toList,
        Nil,
        "expiry of the initialization wait must not report a failure",
      )
      assertEquals(failureRef(consumer).get(), null, "expiry must not latch a consumer failure")
    }
  }

  test("a topic requested after the initialization wait expired is still accepted") {
    val failures = new CopyOnWriteArrayList[Exception]()
    val consumer = newConsumerWithInitWait(failures.add(_), 100.millis)

    runningConsumer(consumer) {
      Thread.sleep(1_000)

      val readiness = consumer.requestTopicSubscription("late-topic")

      assert(
        !readiness.isCompletedExceptionally,
        "a topic requested long after the wait expired must not be refused",
      )
      assertEquals(failures.size(), 0, "accepting a late topic must not report a failure")
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
