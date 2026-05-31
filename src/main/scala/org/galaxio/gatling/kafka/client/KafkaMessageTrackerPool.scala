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

  // consumerTopic -> (matcherKey -> TrackerEntry)
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

    // Fast path: outer computeIfPresent holds the bin lock while we touch the inner
    // map, so a concurrent releaseTracker cannot remove the outer entry in between.
    var found: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    trackers.computeIfPresent(
      consumerTopic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          mKey,
          (_, entry) => {
            entry.refCount.incrementAndGet()
            found = entry.actor
            entry
          },
        )
        innerMap
      },
    )
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

    // Atomic insert-or-increment under the outer bin lock so a concurrent
    // releaseTracker cannot remove the outer entry between get-or-create and insert.
    var result: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    trackers.compute(
      consumerTopic,
      (_, existing) => {
        val innerMap = if (existing != null) existing else new ConcurrentHashMap[String, TrackerEntry]()
        innerMap.compute(
          mKey,
          (_, entry) => {
            if (entry != null) {
              entry.refCount.incrementAndGet()
              result = entry.actor
              entry
            } else {
              result = newActor
              TrackerEntry(newActor, new AtomicInteger(1))
            }
          },
        )
        innerMap
      },
    )
    result
  }

  def releaseTracker(consumerTopic: String, messageMatcher: KafkaMatcher): Unit = {
    val mKey          = matcherKey(messageMatcher)
    var doUnsubscribe = false
    trackers.computeIfPresent(
      consumerTopic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          mKey,
          (_, entry) => {
            if (entry.refCount.decrementAndGet() <= 0) null
            else entry
          },
        )
        if (innerMap.isEmpty) {
          doUnsubscribe = true
          null
        } else innerMap
      },
    )
    if (doUnsubscribe) {
      consumer.removeTopicSubscription(consumerTopic)
    }
  }
}
