package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.{ActorRef, ActorSystem}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.MessageConsumed
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.{KafkaProtocolMessage, KafkaSerdesImplicits}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}
import scala.concurrent.duration.FiniteDuration

object KafkaMessageTrackerPool {

  def apply(
      consumerSettings: Map[String, AnyRef],
      actorSystem: ActorSystem,
      statsEngine: StatsEngine,
      clock: Clock,
  ): Option[KafkaMessageTrackerPool] =
    Option.when(consumerSettings.contains(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))(
      new KafkaMessageTrackerPool(consumerSettings, actorSystem, statsEngine, clock),
    )

}

final class KafkaMessageTrackerPool(
    consumerSettings: Map[String, AnyRef],
    actorSystem: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaLogging with NameGen with KafkaSerdesImplicits {

  private case class TrackerEntry(
      actor: ActorRef[KafkaMessageTracker.TrackerMessage],
      refCount: AtomicInteger,
  )

  /** Two-level map: consumerTopic -> (matcherKey -> TrackerEntry).
    *
    * The outer key is the Kafka topic so that the consumer callback can fan-out a received record to every tracker listening on
    * that topic. The inner key incorporates the matcher's identity so that two request-reply flows sharing the same reply topic
    * but using different [[KafkaMatcher]] instances get independent trackers.
    */
  private val trackers    = new ConcurrentHashMap[String, ConcurrentHashMap[String, TrackerEntry]]
  private val trackerName = "kafkaTracker"

  private def matcherKey(messageMatcher: KafkaMatcher): String =
    s"${messageMatcher.getClass.getName}@${System.identityHashCode(messageMatcher)}"

  // Per-instance executor so shutdown of one pool doesn't affect other pools or subsequent simulations.
  private val consumerExecutor: ExecutorService = Executors.newSingleThreadExecutor()

  private val consumer: DynamicKafkaConsumer[Array[Byte], Array[Byte]] =
    DynamicKafkaConsumer(
      if (consumerSettings.contains(ConsumerConfig.GROUP_ID_CONFIG))
        consumerSettings
      else
        consumerSettings + (ConsumerConfig.GROUP_ID_CONFIG -> s"gatling-kafka-test-${java.util.UUID.randomUUID()}"),
      Set.empty,
      record => {
        val kafkaProtocolMessage = KafkaProtocolMessage.from(record, None)
        val receivedTimestamp    = clock.nowMillis
        Option(trackers.get(record.topic())).foreach { innerMap =>
          innerMap.values().forEach { entry =>
            entry.actor ! MessageConsumed(
              receivedTimestamp,
              kafkaProtocolMessage,
            )
          }
        }
      },
      exception => logger.error(exception.getMessage, exception),
    )

  private val consumerFuture = consumerExecutor.submit(consumer)
  actorSystem.registerOnTermination {
    logger.debug("Closing consumer {}", consumer)
    consumer.close()
    try {
      consumerFuture.get()
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
    }
    consumerExecutor.shutdown()
  }

  private def withProducerTopic(producerTopic: String): KafkaProtocolMessage => KafkaProtocolMessage =
    _.copy(producerTopic = producerTopic)

  def tracker(
      producerTopic: String,
      consumerTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      timeout: FiniteDuration,
  ): ActorRef[KafkaMessageTracker.TrackerMessage] = {

    val mKey = matcherKey(messageMatcher)

    // Fast path: atomic get-and-increment via computeIfPresent on the inner map —
    // avoids holding the CHM bin lock during the blocking addTopicForSubscription
    // in the slow path, while ensuring the refcount increment is atomic with the
    // presence check (no race window with a concurrent releaseTracker that removes
    // the entry).
    var found: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    val innerMap                                            = trackers.get(consumerTopic)
    if (innerMap != null) {
      innerMap.computeIfPresent(
        mKey,
        (_, entry) => {
          entry.refCount.incrementAndGet()
          found = entry.actor
          entry
        },
      )
    }
    if (found != null) return found

    // Slow path: subscribe first (blocking, no CHM lock held), then create actor and register.
    logger.debug(
      "Creating new tracker for topic {} matcher {}, there are currently {} other topic entries",
      consumerTopic,
      mKey,
      trackers.size(),
    )
    val assigned        = consumer.addTopicForSubscription(consumerTopic, timeout)
    if (!assigned) {
      throw new RuntimeException(
        s"Timed out waiting for consumer assignment to topic '$consumerTopic' after $timeout",
      )
    }
    val name            = genName(trackerName)
    val transformations =
      responseTransformer.fold(withProducerTopic(producerTopic))(_.compose(withProducerTopic(producerTopic)))
    val newActor        = actorSystem.actorOf(
      KafkaMessageTracker.actor(
        name,
        statsEngine,
        clock,
        messageMatcher,
        Option(transformations),
      ),
    )

    // Atomic insert-or-increment: if a concurrent thread won the race and already
    // registered a tracker for this (topic, matcher), increment their refcount and
    // use their actor. The losing thread's actor receives no messages and is GC'd
    // at actor system termination.
    var result: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    val topicMap                                             =
      trackers.computeIfAbsent(consumerTopic, _ => new ConcurrentHashMap[String, TrackerEntry]())
    topicMap.compute(
      mKey,
      (_, existing) => {
        if (existing != null) {
          existing.refCount.incrementAndGet()
          result = existing.actor
          existing
        } else {
          result = newActor
          TrackerEntry(newActor, new AtomicInteger(1))
        }
      },
    )
    result
  }

  def releaseTracker(consumerTopic: String, messageMatcher: KafkaMatcher): Unit = {
    val mKey      = matcherKey(messageMatcher)
    // Atomic decrement-and-remove: computeIfPresent serialises with the fast-path
    // computeIfPresent and the slow-path compute, so no window exists where a
    // concurrent tracker() call can observe a stale entry.
    var doCleanup = false
    val innerMap  = trackers.get(consumerTopic)
    if (innerMap != null) {
      innerMap.computeIfPresent(
        mKey,
        (_, entry) => {
          if (entry.refCount.decrementAndGet() <= 0) {
            doCleanup = true
            null
          } else entry
        },
      )
      // If the inner map is now empty, remove the topic entry and unsubscribe.
      if (doCleanup && innerMap.isEmpty) {
        // Remove the outer entry only if it's still the same (empty) inner map.
        trackers.remove(consumerTopic, innerMap)
        consumer.removeTopicSubscription(consumerTopic)
      }
    }
  }
}
