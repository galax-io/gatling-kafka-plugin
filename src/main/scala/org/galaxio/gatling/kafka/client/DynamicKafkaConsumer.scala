package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.jdk.CollectionConverters._

object DynamicKafkaConsumer {

  def apply[K, V](
      settingsMap: Map[String, AnyRef],
      topics: Set[String],
      onRecord: ConsumerRecord[K, V] => Unit,
      onFailure: Exception => Unit,
  ): DynamicKafkaConsumer[K, V] = {
    val settings = new Properties()
    settings.putAll(settingsMap.asJava)
    new DynamicKafkaConsumer[K, V](settings, topics, onRecord, onFailure)
  }
}

final class DynamicKafkaConsumer[K, V] private (
    settings: Properties,
    topics: Set[String],
    onRecord: ConsumerRecord[K, V] => Unit,
    onFailure: Exception => Unit,
) extends Runnable with AutoCloseable {

  private val topicsQueue: java.util.Queue[String] = new ConcurrentLinkedQueue[String]()
  topicsQueue.addAll(topics.asJava)

  private val running: AtomicBoolean        = new AtomicBoolean(true)
  private val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](settings)
  private val initLatch: CountDownLatch     = if (this.topicsQueue.isEmpty) new CountDownLatch(1) else new CountDownLatch(0)

  def addTopicForSubscription(newTopic: String): Unit = {
    this.topicsQueue.add(newTopic)
    if (initLatch.getCount > 0) { // need for staring processing loop
      initLatch.countDown()
    }
  }

  private def getTopicsForSubscription: java.util.Set[String] = {
    if (!this.topicsQueue.isEmpty) {
      val forSubscribe = new java.util.HashSet[String]()
      while (!this.topicsQueue.isEmpty)
        forSubscribe.add(this.topicsQueue.poll)
      if (this.consumer.subscription.containsAll(forSubscribe))
        return java.util.Set.of()
      forSubscribe.addAll(this.consumer.subscription)
      return java.util.Collections.unmodifiableSet(forSubscribe)
    }
    java.util.Set.of()
  }

  override def run(): Unit = {
    try {
      this.initLatch.await()
      this.consumer.subscribe(getTopicsForSubscription)
      while (running.get) {
        val records = this.consumer.poll(Duration.ofMillis(1000))
        records.forEach(record =>
          try this.onRecord(record)
          catch {
            case e: Exception =>
              this.onFailure(e)
          },
        )

        val topicsForSubscription = getTopicsForSubscription
        if (!topicsForSubscription.isEmpty) {
          consumer.subscribe(topicsForSubscription) // Subscribe to all

        }
      }
    } catch {
      case e: WakeupException =>
        // Ignore exception if closing
        // rethrow when someone call wakeup while it is working
        if (running.get) throw e
      case e: Exception       =>
        // unexpected exception
        throw new RuntimeException(e)
    } finally {
      this.topicsQueue.clear()
      consumer.close()
    }
  }

  override def close(): Unit = {
    this.running.set(false)
    if (this.initLatch.getCount > 0) {
      this.initLatch.countDown()
    }
    this.consumer.wakeup()
  }
}
