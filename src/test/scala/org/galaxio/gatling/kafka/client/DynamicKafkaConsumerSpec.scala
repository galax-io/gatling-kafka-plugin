package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.AtomicReference

class DynamicKafkaConsumerSpec extends munit.FunSuite {

  test("removeTopicSubscription queues topic for removal") {
    val consumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      _ => (),
    )
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

  test("addTopicForSubscription queues topic with latch") {
    val consumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      _ => (),
    )
    try {
      val field = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("topicsQueue")
      field.setAccessible(true)
      val queue =
        field.get(consumer).asInstanceOf[java.util.Queue[(String, java.util.concurrent.CountDownLatch)]]

      val initialSize = queue.size()

      val thread = new Thread(() => {
        consumer.addTopicForSubscription("new-topic", scala.concurrent.duration.DurationInt(1).second)
      })
      thread.start()
      Thread.sleep(100)

      assertEquals(queue.size(), initialSize + 1)
      thread.join(2000)
    } finally {
      consumer.close()
    }
  }

  test("addTopicForSubscription fails fast after consumer failure") {
    val consumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers"  -> "localhost:0",
        "key.deserializer"   -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ),
      Set.empty,
      _ => (),
      _ => (),
    )
    try {
      val queueField = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("topicsQueue")
      queueField.setAccessible(true)
      val queue      =
        queueField.get(consumer).asInstanceOf[java.util.Queue[(String, java.util.concurrent.CountDownLatch)]]

      val failureField    = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("consumerFailure")
      failureField.setAccessible(true)
      val consumerFailure = failureField.get(consumer).asInstanceOf[AtomicReference[Exception]]

      @volatile var thrown: IllegalStateException = null

      val thread = new Thread(() => {
        try {
          consumer.addTopicForSubscription("new-topic", scala.concurrent.duration.DurationInt(10).seconds)
        } catch {
          case e: IllegalStateException => thrown = e
        }
      })

      thread.start()

      eventually(1000) {
        assertEquals(queue.size(), 1)
      }

      consumerFailure.set(new RuntimeException("boom"))
      val (_, latch) = queue.peek()
      latch.countDown()

      thread.join(2000)

      assert(!thread.isAlive)
      assert(thrown != null)
      assert(thrown.getCause != null)
      assertEquals(thrown.getCause.getMessage, "boom")
    } finally {
      consumer.close()
    }
  }

  private def eventually(timeoutMillis: Long)(assertion: => Unit): Unit = {
    val deadline                  = System.currentTimeMillis() + timeoutMillis
    var lastError: AssertionError = null
    while (System.currentTimeMillis() < deadline) {
      try {
        assertion
        return
      } catch {
        case e: AssertionError =>
          lastError = e
          Thread.sleep(10)
      }
    }
    if (lastError != null) throw lastError
  }
}
