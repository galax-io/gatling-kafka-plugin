package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaSender}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.testcontainers.utility.DockerImageName

import java.util.Collections
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class KafkaIntegrationSpec extends munit.FunSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def =
    KafkaContainer.Def(
      DockerImageName.parse("confluentinc/cp-kafka:7.9.5").asCompatibleSubstituteFor("apache/kafka"),
    )

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  private def consumerSettings(bootstrap: String, groupId: String): Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
    ConsumerConfig.GROUP_ID_CONFIG                 -> groupId,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
  )

  private def awaitConsumerReady(
      sender: KafkaSender,
      topic: String,
      ready: CountDownLatch,
      timeoutSeconds: Int = 30,
  ): Unit = {
    val probe    = KafkaProtocolMessage("_probe".getBytes, "_probe".getBytes, topic, topic)
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (ready.getCount > 0 && System.currentTimeMillis() < deadline) {
      val l = new CountDownLatch(1)
      sender.send(probe)(_ => l.countDown(), _ => l.countDown())
      l.await(2, TimeUnit.SECONDS)
      ready.await(500, TimeUnit.MILLISECONDS)
    }
    assert(ready.getCount == 0, s"Consumer not ready within ${timeoutSeconds}s")
  }

  private def createTopic(bootstrap: String, topicName: String, partitions: Int = 1): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      ).asJava,
    )
    try {
      admin
        .createTopics(Collections.singletonList(new NewTopic(topicName, partitions, 1.toShort)))
        .all()
        .get(30, TimeUnit.SECONDS)
    } finally admin.close()
  }

  test("KafkaSender sends message to broker and consumer receives it") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "test-send-receive"
      createTopic(bootstrap, topic)

      val sender = KafkaSender(producerSettings(bootstrap))

      val consumerReady = new CountDownLatch(1)
      val received      = new CountDownLatch(1)
      val receivedValue = new AtomicReference[Array[Byte]]()

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-send-receive"),
        Set(topic),
        record => {
          consumerReady.countDown()
          if (new String(record.value()) != "_probe") {
            receivedValue.set(record.value())
            received.countDown()
          }
        },
        e => fail(s"Consumer error: ${e.getMessage}"),
      )
      val consumerThread = new Thread(consumer, "test-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      try {
        awaitConsumerReady(sender, topic, consumerReady)

        val msg = KafkaProtocolMessage(
          key = "key-1".getBytes,
          value = "hello-kafka".getBytes,
          producerTopic = topic,
          consumerTopic = topic,
        )

        val sendLatch = new CountDownLatch(1)
        val sendError = new AtomicReference[Throwable]()
        sender.send(msg)(
          _ => sendLatch.countDown(),
          e => { sendError.set(e); sendLatch.countDown() },
        )

        assert(sendLatch.await(10, TimeUnit.SECONDS), "Send did not complete in time")
        assert(sendError.get() == null, s"Send failed: ${Option(sendError.get()).map(_.getMessage)}")

        assert(received.await(30, TimeUnit.SECONDS), "Message not received within 30s")
        assertEquals(new String(receivedValue.get()), "hello-kafka")
      } finally {
        consumer.close()
        consumerThread.join(5000)
        sender.close()
      }
    }
  }

  test("request-reply: producer sends to input topic, consumer reads from output topic") {
    withContainers { kafka =>
      val bootstrap   = kafka.bootstrapServers
      val inputTopic  = "test-rr-input"
      val outputTopic = "test-rr-output"
      createTopic(bootstrap, inputTopic)
      createTopic(bootstrap, outputTopic)

      val sender = KafkaSender(producerSettings(bootstrap))

      val replyReady    = new CountDownLatch(1)
      val replyReceived = new CountDownLatch(1)
      val replyKey      = new AtomicReference[Array[Byte]]()
      val replyValue    = new AtomicReference[Array[Byte]]()

      val replyConsumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-rr-reply"),
        Set(outputTopic),
        record => {
          replyReady.countDown()
          val v = new String(record.value())
          if (v != "_probe") {
            replyKey.set(record.key())
            replyValue.set(record.value())
            replyReceived.countDown()
          }
        },
        e => fail(s"Reply consumer error: ${e.getMessage}"),
      )
      val replyThread   = new Thread(replyConsumer, "reply-consumer")
      replyThread.setDaemon(true)
      replyThread.start()

      val responderReady = new CountDownLatch(1)

      val responder       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-rr-responder"),
        Set(inputTopic),
        record => {
          responderReady.countDown()
          val v = new String(record.value())
          if (v != "_probe") {
            val responseMsg = KafkaProtocolMessage(
              key = record.key(),
              value = s"reply-to-$v".getBytes,
              producerTopic = outputTopic,
              consumerTopic = outputTopic,
            )
            val latch       = new CountDownLatch(1)
            sender.send(responseMsg)(_ => latch.countDown(), e => { fail(s"Responder send failed: $e"); latch.countDown() })
            latch.await(10, TimeUnit.SECONDS)
          }
        },
        e => fail(s"Responder consumer error: ${e.getMessage}"),
      )
      val responderThread = new Thread(responder, "responder-consumer")
      responderThread.setDaemon(true)
      responderThread.start()

      try {
        awaitConsumerReady(sender, outputTopic, replyReady)
        awaitConsumerReady(sender, inputTopic, responderReady)

        val msg = KafkaProtocolMessage(
          key = "rr-key".getBytes,
          value = "request-payload".getBytes,
          producerTopic = inputTopic,
          consumerTopic = inputTopic,
        )

        val sendLatch = new CountDownLatch(1)
        sender.send(msg)(_ => sendLatch.countDown(), e => { fail(s"Send failed: $e"); sendLatch.countDown() })
        assert(sendLatch.await(10, TimeUnit.SECONDS))

        assert(replyReceived.await(30, TimeUnit.SECONDS), "Reply not received within 30s")
        assertEquals(new String(replyKey.get()), "rr-key")
        assertEquals(new String(replyValue.get()), "reply-to-request-payload")
      } finally {
        responder.close()
        responderThread.join(5000)
        replyConsumer.close()
        replyThread.join(5000)
        sender.close()
      }
    }
  }

  test("dynamic topic subscription: consumer subscribes to new topic at runtime") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val initialTopic = "test-dynamic-initial"
      val dynamicTopic = "test-dynamic-new"
      createTopic(bootstrap, initialTopic)
      createTopic(bootstrap, dynamicTopic)

      val consumerReady  = new CountDownLatch(1)
      val receivedTopics = new ConcurrentLinkedQueue[String]()
      val dynamicLatch   = new CountDownLatch(1)

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-dynamic"),
        Set(initialTopic),
        record => {
          consumerReady.countDown()
          receivedTopics.add(record.topic())
          if (record.topic() == dynamicTopic) dynamicLatch.countDown()
        },
        e => fail(s"Consumer error: ${e.getMessage}"),
      )
      val consumerThread = new Thread(consumer, "dynamic-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      val sender = KafkaSender(producerSettings(bootstrap))

      try {
        awaitConsumerReady(sender, initialTopic, consumerReady)

        val assigned = consumer.addTopicForSubscription(dynamicTopic, 30.seconds)
        assert(assigned, "Dynamic topic subscription timed out")

        val msg = KafkaProtocolMessage(
          key = "dk".getBytes,
          value = "dynamic-value".getBytes,
          producerTopic = dynamicTopic,
          consumerTopic = dynamicTopic,
        )

        val sendLatch = new CountDownLatch(1)
        sender.send(msg)(_ => sendLatch.countDown(), e => { fail(s"Send failed: $e"); sendLatch.countDown() })
        assert(sendLatch.await(10, TimeUnit.SECONDS))

        assert(dynamicLatch.await(30, TimeUnit.SECONDS), "Message on dynamic topic not received")
        assert(receivedTopics.contains(dynamicTopic))
      } finally {
        consumer.close()
        consumerThread.join(5000)
        sender.close()
      }
    }
  }

  test("unsubscribe from topic stops message delivery") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "test-unsubscribe"
      createTopic(bootstrap, topic)

      val consumerReady = new CountDownLatch(1)
      val messageCount  = new java.util.concurrent.atomic.AtomicInteger(0)
      val firstReceived = new CountDownLatch(1)

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-unsub"),
        Set(topic),
        record => {
          consumerReady.countDown()
          if (new String(record.value()) != "_probe") {
            messageCount.incrementAndGet()
            firstReceived.countDown()
          }
        },
        _ => (),
      )
      val consumerThread = new Thread(consumer, "unsub-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      val sender = KafkaSender(producerSettings(bootstrap))

      try {
        awaitConsumerReady(sender, topic, consumerReady)

        val msg1 = KafkaProtocolMessage("k".getBytes, "before-unsub".getBytes, topic, topic)
        val l1   = new CountDownLatch(1)
        sender.send(msg1)(_ => l1.countDown(), _ => l1.countDown())
        l1.await(10, TimeUnit.SECONDS)

        assert(firstReceived.await(30, TimeUnit.SECONDS), "First message not received")

        consumer.removeTopicSubscription(topic)
        // Must wait for poll cycle to process the unsubscribe
        Thread.sleep(3000)

        val countAfterUnsub = messageCount.get()

        val msg2 = KafkaProtocolMessage("k".getBytes, "after-unsub".getBytes, topic, topic)
        val l2   = new CountDownLatch(1)
        sender.send(msg2)(_ => l2.countDown(), _ => l2.countDown())
        l2.await(10, TimeUnit.SECONDS)
        Thread.sleep(5000)

        assertEquals(messageCount.get(), countAfterUnsub, "Messages received after unsubscribe")
      } finally {
        consumer.close()
        consumerThread.join(5000)
        sender.close()
      }
    }
  }

  test("concurrent sends: multiple messages sent in parallel arrive correctly") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "test-concurrent"
      createTopic(bootstrap, topic, partitions = 3)

      val messageCount   = 50
      val consumerReady  = new CountDownLatch(1)
      val receivedValues = new ConcurrentLinkedQueue[String]()
      val allReceived    = new CountDownLatch(messageCount)

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-concurrent"),
        Set(topic),
        record => {
          consumerReady.countDown()
          val v = new String(record.value())
          if (v != "_probe") {
            receivedValues.add(v)
            allReceived.countDown()
          }
        },
        e => fail(s"Consumer error: ${e.getMessage}"),
      )
      val consumerThread = new Thread(consumer, "concurrent-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      val sender = KafkaSender(producerSettings(bootstrap))

      try {
        awaitConsumerReady(sender, topic, consumerReady)

        val sendLatch  = new CountDownLatch(messageCount)
        val sendErrors = new ConcurrentLinkedQueue[Throwable]()

        (0 until messageCount).foreach { i =>
          val msg = KafkaProtocolMessage(s"key-$i".getBytes, s"value-$i".getBytes, topic, topic)
          sender.send(msg)(_ => sendLatch.countDown(), e => { sendErrors.add(e); sendLatch.countDown() })
        }

        assert(sendLatch.await(30, TimeUnit.SECONDS), "Not all sends completed")
        assert(sendErrors.isEmpty, s"Send errors: ${sendErrors.asScala.map(_.getMessage).mkString(", ")}")

        assert(
          allReceived.await(60, TimeUnit.SECONDS),
          s"Only received ${messageCount - allReceived.getCount}/$messageCount messages",
        )

        val received = receivedValues.asScala.toSet
        (0 until messageCount).foreach { i =>
          assert(received.contains(s"value-$i"), s"Missing value-$i")
        }
      } finally {
        consumer.close()
        consumerThread.join(5000)
        sender.close()
      }
    }
  }

  test("consumer close shuts down cleanly") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "test-cleanup"
      createTopic(bootstrap, topic)

      val consumerReady = new CountDownLatch(1)

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-cleanup"),
        Set(topic),
        _ => consumerReady.countDown(),
        e => fail(s"Consumer error: ${e.getMessage}"),
      )
      val consumerThread = new Thread(consumer, "cleanup-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      val sender = KafkaSender(producerSettings(bootstrap))
      try awaitConsumerReady(sender, topic, consumerReady)
      finally sender.close()

      consumer.close()
      consumerThread.join(10000)
      assert(!consumerThread.isAlive, "Consumer thread did not terminate after close")
    }
  }

  test("KafkaSender close shuts down producer cleanly") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "test-sender-cleanup"
      createTopic(bootstrap, topic)

      val sender = KafkaSender(producerSettings(bootstrap))

      val msg   = KafkaProtocolMessage("k".getBytes, "v".getBytes, topic, topic)
      val latch = new CountDownLatch(1)
      sender.send(msg)(_ => latch.countDown(), e => { fail(s"Send failed: $e"); latch.countDown() })
      assert(latch.await(10, TimeUnit.SECONDS))

      sender.close()

      intercept[IllegalStateException] {
        val l2 = new CountDownLatch(1)
        sender.send(msg)(_ => l2.countDown(), _ => l2.countDown())
      }
    }
  }

  test("addTopicForSubscription with very short timeout returns false") {
    withContainers { kafka =>
      val bootstrap  = kafka.bootstrapServers
      val topic      = "test-sub-timeout"
      val setupTopic = "test-sub-timeout-setup"
      createTopic(bootstrap, setupTopic)

      val consumerReady = new CountDownLatch(1)

      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap, "group-sub-timeout"),
        Set(setupTopic),
        _ => consumerReady.countDown(),
        _ => (),
      )
      val consumerThread = new Thread(consumer, "timeout-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      val sender = KafkaSender(producerSettings(bootstrap))

      try {
        awaitConsumerReady(sender, setupTopic, consumerReady)
        sender.close()
        // 1 nanosecond timeout — subscription cannot complete in time
        val assigned = consumer.addTopicForSubscription(topic, scala.concurrent.duration.Duration.fromNanos(1))
        assert(!assigned, "Expected subscription to time out")
      } finally {
        consumer.close()
        consumerThread.join(5000)
      }
    }
  }
}
