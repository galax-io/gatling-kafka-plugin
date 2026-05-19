package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.ActorSystem
import io.gatling.core.stats.StatsEngine

import java.util.concurrent.ConcurrentHashMap

trait KafkaTrackerProviderFactory {
  def trackerProvider(consumeSettings: Map[String, AnyRef]): KafkaTrackerProvider
}

class KafkaTrackersPoolFactory(
    system: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaTrackerProviderFactory {

  private val providers = new ConcurrentHashMap[Map[String, AnyRef], KafkaTrackerProvider]()

  override def trackerProvider(consumeSettings: Map[String, AnyRef]): KafkaTrackerProvider =
    providers.computeIfAbsent(
      consumeSettings,
      new TrackersPool(_, system, statsEngine, clock),
    )
}
