package org.galaxio.gatling.kafka.client

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
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
  private val initializationTimeout = 90.seconds
  private val defaultAssignTimeout  = 60.seconds
}

final class DynamicKafkaConsumer[K, V] private (
    settings: Properties,
    topics: Set[String],
    onRecord: ConsumerRecord[K, V] => Unit,
    onFailure: Exception => Unit,
) extends Runnable with AutoCloseable with StrictLogging {

  private val topicsQueue: java.util.Queue[(String, CountDownLatch)] = new ConcurrentLinkedQueue[(String, CountDownLatch)]()
  topicsQueue.addAll(topics.map((_, new CountDownLatch(0))).asJava)

  private val topicsToRemove: java.util.Queue[String] = new ConcurrentLinkedQueue[String]()

  private val running: AtomicBoolean        = new AtomicBoolean(true)
  private val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](settings)
  private val initLatch: CountDownLatch     = if (this.topicsQueue.isEmpty) new CountDownLatch(1) else new CountDownLatch(0)

  def removeTopicSubscription(topic: String): Unit =
    topicsToRemove.add(topic)

  def addTopicForSubscription(
      newTopic: String,
      assignTimeout: FiniteDuration = DynamicKafkaConsumer.defaultAssignTimeout,
  ): Boolean = {
    val latch = new CountDownLatch(1)
    this.topicsQueue.add(newTopic, latch)
    if (initLatch.getCount > 0) { // need for starting processing loop
      initLatch.countDown()
    }
    latch.await(assignTimeout.length, assignTimeout.unit)
  }

  /** Applies pending topic additions and removals in a single consumer.subscribe call so the ConsumerRebalanceListener is never
    * overwritten between the two operations.
    */
  private def updateSubscription(): Unit = {
    val toRemove = mutable.Set.empty[String]
    while (!topicsToRemove.isEmpty) {
      toRemove.add(topicsToRemove.poll())
    }

    if (topicsQueue.isEmpty && toRemove.isEmpty) return

    val currentSubscription = consumer.subscription()
    val forNewTopics        = mutable.Set.empty[(String, CountDownLatch)]

    while (!topicsQueue.isEmpty) {
      val (topic, latch) = topicsQueue.poll()
      if (currentSubscription.contains(topic))
        latch.countDown()
      else
        forNewTopics.add((topic, latch))
    }

    if (forNewTopics.isEmpty && toRemove.isEmpty) return

    val baseTopics = currentSubscription.asScala.toSet -- toRemove
    val allTopics  = baseTopics ++ forNewTopics.map(_._1)
    val newLatches = forNewTopics.map(_._2)

    if (allTopics.isEmpty) {
      if (currentSubscription.nonEmpty) consumer.unsubscribe()
      return
    }

    consumer.subscribe(
      allTopics.asJava,
      new ConsumerRebalanceListener {
        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
          logger.debug(s"revoked partitions $partitions")

        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          logger.debug(s"assigned partitions $partitions")
          newLatches.foreach(_.countDown())
        }
      },
    )
  }

  override def run(): Unit = {
    try {
      val timeout = DynamicKafkaConsumer.initializationTimeout
      this.initLatch.await(timeout.length, timeout.unit)
      updateSubscription()
      while (running.get) {
        val records = this.consumer.poll(Duration.ofMillis(1000))
        records.forEach(record =>
          try this.onRecord(record)
          catch {
            case e: Exception =>
              this.onFailure(e)
          },
        )
        updateSubscription()
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
