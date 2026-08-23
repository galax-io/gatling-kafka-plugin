package org.galaxio.gatling.kafka.client

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

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

  /** Backstop for an idle turn, in case a signal is ever missed.
    *
    * Not the mechanism: `requestTopicSubscription`, `removeTopicSubscription` and `close` all wake the waiting turn directly,
    * so a newly requested topic is picked up at once and shutdown is not delayed. Kept long rather than short precisely because
    * nothing depends on it — an idle consumer should cost nothing.
    */
  private val idleBackoffMillis: Long = 1000L

  /** Total budget for resolving fetch positions in one pass, across **all** assigned partitions.
    *
    * Bounded because the pass runs on the single thread that timestamps every reply for every topic on this consumer, and it
    * can run inside `poll()` via the rebalance listener: an unbounded wait there stalls the whole run, and a per-partition
    * budget would multiply by the assignment width and blow `max.poll.interval.ms` on a wide topic.
    */
  private val positionTimeoutMillis: Long = 10000L

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

  /** Whether the consumer currently holds a subscription, tracked here rather than read back off the consumer.
    *
    * Only ever `subscribe` is called — never `assign` — so this is exactly the condition under which `poll` would throw.
    */
  @volatile private var subscribedToAnything: Boolean = false

  /** Signalled when there is work to do or the consumer is closing, so an idle turn does not have to wait out a fixed backoff.
    */
  private val idleSignal = new Object

  private val running: AtomicBoolean                      = new AtomicBoolean(true)
  private val consumer: KafkaConsumer[K, V]               = new KafkaConsumer[K, V](settings)
  private val consumerFailure: AtomicReference[Exception] = new AtomicReference[Exception](null)

  def removeTopicSubscription(topic: String): Unit = {
    topicsToRemove.add(topic)
    signalWork()
  }

  /** Requests that `topic` joins the subscription. Returns immediately; the future completes once the consumer holds an
    * assignment covering the topic, and fails if the consumer failed or was closed before that happened. The future carries no
    * deadline of its own — callers own their timeout policy.
    */
  def requestTopicSubscription(topic: String): CompletableFuture[Void] = {
    val readiness = new CompletableFuture[Void]()
    // Rejected here rather than allowed through to `subscribe`, which throws
    // "Topic collection to subscribe to cannot contain null or empty topic" on the consumer thread —
    // where the catch-all turns it into a permanent consumer failure and kills request-reply for every
    // scenario sharing this consumer. One virtual user whose reply-topic expression resolves to blank
    // should fail that user, not the run.
    if (topic == null || topic.trim.isEmpty) {
      readiness.completeExceptionally(
        new IllegalArgumentException("Reply topic is null or blank; check the replyTopic expression for this request"),
      )
      return readiness
    }
    subscriptionUnavailable match {
      case Some(cause) => readiness.completeExceptionally(cause)
      case None        =>
        this.topicsQueue.add((topic, readiness))
        signalWork()
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

  /** Resolves the fetch position for **every** assigned partition, then completes readiness for the topics awaiting it.
    *
    * Assignment alone is not enough. It precedes fetch-position resolution, and the plugin defaults `auto.offset.reset` to
    * `latest`, so a reply produced between the two is skipped: the position resolves to the log end *after* that record, the
    * request never sees its answer, and it fails on the reply timeout — indistinguishable from a system under test that never
    * replied (issue #193).
    *
    * Positions are resolved for the whole assignment rather than only for awaited topics, because readiness is not a one-shot
    * property. The plugin calls `subscribe` on every reply-topic acquisition and every idle removal, and the default eager
    * assignor revokes and re-assigns *all* partitions each time — so a topic that completed readiness earlier gets fresh,
    * unpositioned partitions and would silently reopen the same window. Scoping this to `awaitingAssignment` made the fix hold
    * only until the second acquisition of a run, and leaned entirely on `enable.auto.commit` defaulting to true to restore the
    * position from the pre-revoke commit.
    */
  private def completeAssignedReadiness(): Unit = {
    val assigned = consumer.assignment()
    if (!assigned.isEmpty) {
      val failure = resolvePositions(assigned)
      if (!awaitingAssignment.isEmpty) {
        assigned.asScala.map(_.topic()).toSet.foreach { (topic: String) =>
          val pending = awaitingAssignment.remove(topic)
          if (pending != null) {
            failure match {
              case None        => pending.forEach(_.complete(null))
              case Some(cause) => pending.forEach(_.completeExceptionally(cause))
            }
          }
        }
      }
    }
  }

  /** Resolves the fetch position for the whole assignment under **one** deadline, returning the failure rather than throwing.
    *
    * One budget for the pass, not one per partition. `position` takes a per-call timeout, so looping it handed each partition a
    * fresh 10 s and made the real bound `10 s × partitions` on the single thread that timestamps every reply for every topic —
    * long enough on a wide topic to blow `max.poll.interval.ms` and get the consumer evicted, which is the opposite of what the
    * bound was for. In practice the first call resolves every partition that needs it, so the rest are cache hits.
    *
    * Failure is scoped to the awaiting topics on purpose. Routing it through `markConsumerFailed` would latch `consumerFailure`
    * and refuse every present and future subscription for the rest of the run — the terminal state issue #143 exists to prevent
    * — over what may be one slow `ListOffsets`.
    *
    * `WakeupException` propagates: it is how `close()` interrupts a blocked call, and swallowing it would break shutdown.
    * `InterruptException` deliberately does **not** propagate. It is a `KafkaException`, not a
    * `java.lang.InterruptedException`, so the run loop's interrupt arm never matches it and it would reach the catch-all that
    * latches a permanent consumer failure — again #143, reached by the one exception a rethrow was meant to protect. Treated as
    * a position failure instead; `running` still governs shutdown.
    */
  private def resolvePositions(partitions: util.Collection[TopicPartition]): Option[Throwable] =
    try {
      val deadline = System.currentTimeMillis() + DynamicKafkaConsumer.positionTimeoutMillis
      partitions.forEach { tp =>
        val remaining = math.max(1L, deadline - System.currentTimeMillis())
        consumer.position(tp, Duration.ofMillis(remaining))
      }
      None
    } catch {
      case e: WakeupException =>
        throw e
      case NonFatal(e)        =>
        logger.error(s"Could not resolve a fetch position for $partitions; failing readiness for those topics only", e)
        Some(
          new IllegalStateException(
            s"Reply channel is subscribed to $partitions but its fetch position could not be resolved, " +
              "so a reply published now could be skipped; failing this topic rather than reporting it ready",
            e,
          ),
        )
    }

  private def markConsumerFailed(exception: Exception): Unit = {
    if (consumerFailure.compareAndSet(null, exception)) {
      failPendingSubscriptions(new IllegalStateException(DynamicKafkaConsumer.consumerFailedMessage, exception))
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

    // Emptying the subscription is allowed: `subscribe` with an empty collection is defined as
    // `unsubscribe`, and the poll guard in `run` covers what follows.
    //
    // This used to return early instead, to keep the consumer from ever holding nothing — but by then
    // `toRemove` had already been drained out of `topicsToRemove`, so the early return discarded the
    // whole batch permanently, not just the last topic. The idle sweep emits one removal per emptied
    // topic in a single pass, so a run with a reply topic per virtual user could leave every one of
    // them subscribed and fetched for the rest of the run, unreachable by any later removal.

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
      subscribedToAnything = allTopics.nonEmpty
    }

    completeAssignedReadiness()
  }

  /** Waits until there is something to subscribe to, the consumer is closing, or the backoff elapses.
    *
    * The backoff is only a backstop against a missed signal; `requestTopicSubscription` and `close` both wake this directly.
    */
  private def awaitWork(): Unit =
    idleSignal.synchronized {
      if (running.get && topicsQueue.isEmpty && topicsToRemove.isEmpty) {
        idleSignal.wait(DynamicKafkaConsumer.idleBackoffMillis)
      }
    }

  private def signalWork(): Unit =
    idleSignal.synchronized(idleSignal.notifyAll())

  override def run(): Unit = {
    try {
      updateSubscription()
      while (running.get) {
        // A consumer holding nothing to receive on throws on poll, and that exception reaches
        // markConsumerFailed, which poisons every tracker in the pool for the rest of the run (issue
        // #143). Nothing can arrive in that state anyway, so the turn waits for a subscription request
        // rather than fetching.
        //
        // Read off a flag this class maintains rather than asking the consumer. `subscription()` and
        // `assignment()` are both synchronized on the state the fetcher and heartbeat contend on, and
        // both allocate a fresh Set — per turn, including every turn that carries records, on the one
        // thread that timestamps every reply.
        //
        // The wait is signalled by requestTopicSubscription and by close, so a topic requested here is
        // picked up at once and shutdown is not delayed. A bare sleep was neither: it cost up to a full
        // backoff on both, because wakeup() interrupts a poll but not a sleep.
        if (!subscribedToAnything) {
          awaitWork()
        } else {
          val records = this.consumer.poll(Duration.ofMillis(1000))
          records.forEach(record =>
            try this.onRecord(record)
            catch {
              case e: Exception =>
                this.onFailure(e)
            },
          )
        }
        updateSubscription()
      }
    } catch {
      case e: WakeupException      =>
        // `close()` wakes the consumer to interrupt a blocked poll, so while shutting down this is the
        // expected way out of the loop and is ignored. A wakeup arriving while the consumer is still
        // running has no legitimate source, so it is latched and reported rather than rethrown — the
        // rethrow this comment used to describe went with #128.
        if (running.get) {
          markConsumerFailed(e)
          this.onFailure(e)
        }
      case _: InterruptedException =>
        // A shutdown signal, not a consumer failure. Latching one here would fail every pending
        // readiness and poison the pool for the rest of the run — the terminal state issue #143 exists
        // to prevent — over an interrupt that was asking this thread to stop. The flag is restored so
        // whoever sent it can still observe it.
        logger.debug("Consumer thread interrupted; treating it as a close request")
        running.set(false)
        Thread.currentThread().interrupt()
      case e: Exception            =>
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
    // Wakes an idle turn at once rather than leaving it to wait out the backoff: wakeup() below only
    // interrupts a blocking poll.
    signalWork()
    // The run loop drains the queue when it exits, but close() is also valid on a consumer that was
    // never run: resolve whatever is still pending rather than leaving callers waiting on it.
    failPendingSubscriptions(new IllegalStateException(DynamicKafkaConsumer.consumerClosedMessage))
    this.consumer.wakeup()
  }
}
