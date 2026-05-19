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

  private val running: AtomicBoolean        = new AtomicBoolean(true)
  private val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](settings)
  private val initLatch: CountDownLatch     = if (this.topicsQueue.isEmpty) new CountDownLatch(1) else new CountDownLatch(0)

  def addTopicForSubscription(
      newTopic: String,
      assignTimeout: FiniteDuration = DynamicKafkaConsumer.defaultAssignTimeout,
  ): Unit = {
    val latch = new CountDownLatch(1)
    this.topicsQueue.add(newTopic, latch)
    if (initLatch.getCount > 0) { // need for staring processing loop
      initLatch.countDown()
    }
    latch.await(assignTimeout.length, assignTimeout.unit)
  }

  private def getTopicsForSubscription: Set[(String, CountDownLatch)] = {
    if (!this.topicsQueue.isEmpty) {
      val currentSubscription = this.consumer.subscription()
      val forSubscribe        = mutable.Set.empty[(String, CountDownLatch)]
      while (!this.topicsQueue.isEmpty) {
        val (topic, latch) = this.topicsQueue.poll()
        if (currentSubscription.contains(topic)) {
          latch.countDown()
        } else {
          forSubscribe.add(topic, latch)
        }
      }
      if (forSubscribe.isEmpty)
        return Set.empty

      forSubscribe.addAll(currentSubscription.asScala.map((_, new CountDownLatch(1))))
      return forSubscribe.toSet
    }
    Set.empty
  }

  private def subscribeTopics(forSubscribe: Set[(String, CountDownLatch)]): Unit = {
    if (forSubscribe.nonEmpty) {
      val (topics, latches) =
        forSubscribe.foldLeft((mutable.Set.empty[String], mutable.Set.empty[CountDownLatch]))((result, tl) => {
          val (ts, ls)       = result
          val (topic, latch) = tl
          ts.add(topic)
          ls.add(latch)
          result
        })
      this.consumer.subscribe(
        topics.asJava,
        new ConsumerRebalanceListener {
          override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
            logger.debug(s"revoked partitions $partitions")
          }

          override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
            logger.debug(s"assigned partitions $partitions")
            latches.foreach(_.countDown())
          }
        },
      )
    }
  }

  override def run(): Unit = {
    try {
      val timeout = DynamicKafkaConsumer.initializationTimeout
      this.initLatch.await(timeout.length, timeout.unit)
      subscribeTopics(getTopicsForSubscription)
      while (running.get) {
        val records = this.consumer.poll(Duration.ofMillis(1000))
        records.forEach(record =>
          try this.onRecord(record)
          catch {
            case e: Exception =>
              this.onFailure(e)
          },
        )
        subscribeTopics(getTopicsForSubscription)
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
