package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, CopyOnWriteArrayList, ExecutionException, TimeUnit, TimeoutException}
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

  // Issue #143. A simulation that ramps, warms up, or simply reaches request-reply late leaves this
  // consumer running with nothing requested. It used to reach poll() on a consumer that had never
  // subscribed, which threw and latched a failure that poisoned every tracker for the rest of the run.
  private def newConsumerReporting(onFailure: Exception => Unit): DynamicKafkaConsumer[Array[Byte], Array[Byte]] =
    DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      onFailure,
    )

  private def runningConsumer(
      consumer: DynamicKafkaConsumer[Array[Byte], Array[Byte]],
  )(body: => Unit): Unit = {
    val thread = new Thread(consumer, "idle-consumer")
    thread.setDaemon(true)
    thread.start()
    try body
    finally {
      consumer.close()
      thread.join(10_000)
      assert(!thread.isAlive, "the consumer thread must exit after close")
    }
  }

  test("a consumer running with nothing requested never fails") {
    val failures = new CopyOnWriteArrayList[Exception]()
    val consumer = newConsumerReporting(failures.add(_))

    runningConsumer(consumer) {
      // Long enough that the loop has taken many turns with nothing subscribed.
      Thread.sleep(1_000)

      assertEquals(
        failures.asScala.map(_.getMessage).toList,
        Nil,
        "an idle consumer must not report a failure",
      )
      assertEquals(failureRef(consumer).get(), null, "and must not latch one")
    }
  }

  test("a topic requested long after startup is still accepted") {
    val consumer = newConsumer()

    try {
      // What #143 broke was a *timed* initialization window: it expired, and every reply topic requested
      // afterwards was refused for the rest of the run. So the property is "elapsed time alone never
      // refuses a request", and elapsed time is all this needs to reproduce it.
      //
      // Deliberately without a running poll loop. This used to start one against `localhost:0` and then
      // assert on `readiness` right after requesting — but by then the loop is awake and driving a
      // subscribe against an unreachable broker, so it can fail that subscription asynchronously and
      // legitimately. The assertion was racing it, and under the CPU contention of a full `sbt test` the
      // loop won: the test failed for a reason that has nothing to do with lateness. The idle loop's own
      // behaviour is covered by "a consumer running with nothing requested never fails" above.
      Thread.sleep(1_000)

      val readiness = consumer.requestTopicSubscription("late-topic")

      assert(
        !readiness.isCompletedExceptionally,
        "a topic requested long after startup must not be refused",
      )
      assertEquals(failureRef(consumer).get(), null, "and must not latch a consumer failure")
      assertEquals(
        subscriptionQueue(consumer).size,
        1,
        "the late topic must be queued for the consumer to pick up, not dropped",
      )
    } finally consumer.close()
  }

  test("a blank reply topic fails only that request, not the consumer") {
    val failures = new CopyOnWriteArrayList[Exception]()
    val consumer = newConsumerReporting(failures.add(_))

    runningConsumer(consumer) {
      // Reaches here from a replyTopic expression that resolved to nothing. Passed through to
      // consumer.subscribe it throws on the consumer thread, where the catch-all latches a permanent
      // consumer failure and kills request-reply for every scenario sharing this consumer.
      val readiness = consumer.requestTopicSubscription("   ")
      Thread.sleep(500)

      assert(readiness.isCompletedExceptionally, "a blank topic must fail its own request")
      assertEquals(failureRef(consumer).get(), null, "and must not latch a consumer failure")
      assertEquals(failures.size(), 0, "nor report one")
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
