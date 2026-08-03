package org.galaxio.gatling.kafka.client

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object DynamicKafkaConsumer {

  def apply[K, V](
      settingsMap: Map[String, AnyRef],
      topics: Set[String],
      onRecord: ConsumerRecord[K, V] => Unit,
      onFailure: Exception => Unit,
  ): DynamicKafkaConsumer[K, V]     = {
    val settings = new Properties()
    settings.putAll(settingsMap.asJava)
    new DynamicKafkaConsumer[K, V](settings, topics, onRecord, onFailure)
  }
  private val initializationTimeout = 90.seconds

  private[client] val consumerFailedMessage = "Kafka consumer failed; dynamic consumer can no longer be used"
  private[client] val consumerClosedMessage = "Kafka consumer is closed; topic subscription will not complete"
}

final class DynamicKafkaConsumer[K, V] private (
    settings: Properties,
    topics: Set[String],
    onRecord: ConsumerRecord[K, V] => Unit,
    onFailure: Exception => Unit,
) extends Runnable with AutoCloseable with StrictLogging {

  private val topicsQueue: java.util.Queue[(String, CompletableFuture[Void])] =
    new ConcurrentLinkedQueue[(String, CompletableFuture[Void])]()
  topicsQueue.addAll(topics.map((_, CompletableFuture.completedFuture[Void](null))).asJava)

  private val topicsToRemove: java.util.Queue[String] = new ConcurrentLinkedQueue[String]()

  /** Readiness waiting for its topic to show up in the assignment.
    *
    * Deliberately kept on the consumer rather than captured by the rebalance listener: a later `subscribe` replaces the
    * listener, and anything the previous one was holding would never be signalled.
    */
  private val awaitingAssignment: ConcurrentHashMap[String, java.util.Queue[CompletableFuture[Void]]] =
    new ConcurrentHashMap[String, java.util.Queue[CompletableFuture[Void]]]()

  private val running: AtomicBoolean                      = new AtomicBoolean(true)
  private val consumer: KafkaConsumer[K, V]               = new KafkaConsumer[K, V](settings)
  private val initLatch: CountDownLatch                   = if (this.topicsQueue.isEmpty) new CountDownLatch(1) else new CountDownLatch(0)
  private val consumerFailure: AtomicReference[Exception] = new AtomicReference[Exception](null)

  def removeTopicSubscription(topic: String): Unit =
    topicsToRemove.add(topic)

  /** Requests that `topic` joins the subscription. Returns immediately; the future completes once the consumer holds an
    * assignment covering the topic, and fails if the consumer failed or was closed before that happened. The future carries no
    * deadline of its own — callers own their timeout policy.
    */
  def requestTopicSubscription(topic: String): CompletableFuture[Void] = {
    val readiness = new CompletableFuture[Void]()
    subscriptionUnavailable match {
      case Some(cause) => readiness.completeExceptionally(cause)
      case None        =>
        this.topicsQueue.add((topic, readiness))
        if (initLatch.getCount > 0) {
          initLatch.countDown()
        }
        // A failure or close racing with the enqueue above would have drained the queue before this
        // entry landed in it, leaving the future pending forever; resolve it here instead.
        subscriptionUnavailable.foreach(readiness.completeExceptionally)
    }
    readiness
  }

  private def subscriptionUnavailable: Option[Throwable] = {
    val failure = consumerFailure.get()
    if (failure != null) Some(new IllegalStateException(DynamicKafkaConsumer.consumerFailedMessage, failure))
    else if (!running.get) Some(new IllegalStateException(DynamicKafkaConsumer.consumerClosedMessage))
    else None
  }

  private def failPendingSubscriptions(cause: Throwable): Unit = {
    while (!topicsQueue.isEmpty) {
      val pending = topicsQueue.poll()
      if (pending != null) {
        pending._2.completeExceptionally(cause)
      }
    }
    awaitingAssignment.values().forEach(_.forEach(_.completeExceptionally(cause)))
    awaitingAssignment.clear()
  }

  /** Completes readiness for every topic the consumer currently holds partitions for. Runs on the consumer thread only, since
    * it reads the consumer's assignment.
    */
  private def completeAssignedReadiness(): Unit =
    if (!awaitingAssignment.isEmpty) {
      consumer.assignment().asScala.map(_.topic()).toSet.foreach { (topic: String) =>
        val pending = awaitingAssignment.remove(topic)
        if (pending != null) {
          pending.forEach(_.complete(null))
        }
      }
    }

  private def markConsumerFailed(exception: Exception): Unit = {
    if (consumerFailure.compareAndSet(null, exception)) {
      failPendingSubscriptions(new IllegalStateException(DynamicKafkaConsumer.consumerFailedMessage, exception))
      if (initLatch.getCount > 0) {
        initLatch.countDown()
      }
    }
  }

  /** Applies pending topic additions and removals in a single consumer.subscribe call so the ConsumerRebalanceListener is never
    * overwritten between the two operations.
    */
  private def updateSubscription(): Unit = {
    val toRemove = mutable.Set.empty[String]
    while (!topicsToRemove.isEmpty) {
      toRemove.add(topicsToRemove.poll())
    }

    if (topicsQueue.isEmpty && toRemove.isEmpty) {
      completeAssignedReadiness()
      return
    }

    val requestedTopics = mutable.Set.empty[String]
    while (!topicsQueue.isEmpty) {
      // close() drains this queue too, from its own thread, so poll can come back empty-handed
      // between the check above and here.
      val pending = topicsQueue.poll()
      if (pending != null) {
        val (topic, readiness) = pending
        requestedTopics.add(topic)
        awaitingAssignment
          .computeIfAbsent(topic, _ => new ConcurrentLinkedQueue[CompletableFuture[Void]]())
          .add(readiness)
        // A close() racing the two calls above can drain the map in between, leaving this entry
        // attached to a queue nothing holds any more; resolve it here instead of parking forever.
        subscriptionUnavailable.foreach(readiness.completeExceptionally)
      }
    }

    val currentSubscription = consumer.subscription().asScala.toSet
    val allTopics           = (currentSubscription -- toRemove) ++ requestedTopics

    // Anything dropped from the subscription can never be assigned, so readiness still parked for it
    // would otherwise wait out the caller's full timeout on a topic nobody is listening to.
    toRemove.filterNot(allTopics.contains).foreach { topic =>
      val abandoned = awaitingAssignment.remove(topic)
      if (abandoned != null) {
        abandoned.forEach(
          _.completeExceptionally(
            new IllegalStateException(s"Subscription to topic '$topic' was removed before it was assigned"),
          ),
        )
      }
    }

    if (allTopics.isEmpty) {
      if (currentSubscription.nonEmpty) consumer.unsubscribe()
      return
    }

    // Kafka ignores a subscribe() whose topic set is unchanged, so only call it on a real change.
    // Readiness does not depend on the resulting rebalance: it is resolved from the assignment below.
    if (allTopics != currentSubscription) {
      consumer.subscribe(
        allTopics.asJava,
        new ConsumerRebalanceListener {
          override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
            logger.debug(s"revoked partitions $partitions")

          override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
            logger.debug(s"assigned partitions $partitions")
            completeAssignedReadiness()
          }
        },
      )
    }

    completeAssignedReadiness()
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
        if (running.get) {
          markConsumerFailed(e)
          this.onFailure(e)
        }
      case e: Exception       =>
        // Propagate unexpected exception through the failure callback
        markConsumerFailed(e)
        this.onFailure(e)
    } finally {
      failPendingSubscriptions(new IllegalStateException(DynamicKafkaConsumer.consumerClosedMessage))
      consumer.close()
    }
  }

  override def close(): Unit = {
    this.running.set(false)
    if (this.initLatch.getCount > 0) {
      this.initLatch.countDown()
    }
    // The run loop drains the queue when it exits, but close() is also valid on a consumer that was
    // never run: resolve whatever is still pending rather than leaving callers waiting on it.
    failPendingSubscriptions(new IllegalStateException(DynamicKafkaConsumer.consumerClosedMessage))
    this.consumer.wakeup()
  }
}
