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

  // Trackers map Output Topic (String) to Tracker/Actor
  private val trackers         = new ConcurrentHashMap[String, ActorRef[KafkaMessageTracker.TrackerMessage]]
  private val trackerRefCounts = new ConcurrentHashMap[String, AtomicInteger]()
  private val trackerName      = "kafkaTracker"

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
        val tracker              = Option(trackers.get(record.topic()))

        tracker.map(
          _ ! MessageConsumed(
            receivedTimestamp,
            kafkaProtocolMessage,
          ),
        )
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

    val tracker = trackers.computeIfAbsent(
      consumerTopic,
      _ => {
        logger.debug(
          "Computing new tracker for topic {}, there are currently {} other trackers",
          consumerTopic,
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
        actorSystem.actorOf(
          KafkaMessageTracker.actor(
            name,
            statsEngine,
            clock,
            messageMatcher,
            Option(transformations),
          ),
        )
      },
    )
    // Use compute (not computeIfAbsent+increment) so increment and the decrement in
    // releaseTracker are serialized on the same key, preventing count drift.
    trackerRefCounts.compute(
      consumerTopic,
      (_, existing) => {
        val count = if (existing == null) new AtomicInteger(0) else existing
        count.incrementAndGet()
        count
      },
    )
    tracker
  }

  def releaseTracker(consumerTopic: String): Unit = {
    // var doCleanup is set synchronously inside compute (holds map segment lock) and read
    // only after compute returns — safe without additional synchronization.
    var doCleanup = false
    trackerRefCounts.compute(
      consumerTopic,
      (_, count) => {
        if (count == null || count.decrementAndGet() <= 0) {
          doCleanup = true
          null
        } else count
      },
    )
    if (doCleanup) {
      // A concurrent tracker() call can re-insert into trackers between here and remove().
      // In practice this cannot occur: dynamic topics are unique per request (no concurrent
      // reuse), and static topics keep refcount > 1 throughout the simulation.
      trackers.remove(consumerTopic)
      consumer.removeTopicSubscription(consumerTopic)
    }
  }
}
