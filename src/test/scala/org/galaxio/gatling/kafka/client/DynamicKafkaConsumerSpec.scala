package org.galaxio.gatling.kafka.client

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
}
