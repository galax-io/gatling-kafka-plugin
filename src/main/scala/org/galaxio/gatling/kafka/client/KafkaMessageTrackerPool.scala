package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.{ActorRef, ActorSystem}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{MessageConsumed, TrackerMessage}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.{KafkaProtocolMessage, KafkaSerdesImplicits}

import java.util.concurrent.ConcurrentHashMap
import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaMessageTrackerPool {

  /** Because we may need access to headers whilst deserializing (which can contain deserialization info), we will need to use
    * the Kafka Processor Api
    * https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-process
    */
  private final class KafkaMessageProcessorSupplier(
      tracker: ActorRef[TrackerMessage],
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      clock: Clock,
  ) extends ProcessorSupplier[Array[Byte], Array[Byte], Void, Void] with KafkaLogging {

    override def get(): Processor[Array[Byte], Array[Byte], Void, Void] =
      (record: api.Record[Array[Byte], Array[Byte]]) => {
        val headers = Option(record.headers())
        val key     = record.key()
        val value   = record.value()
        val message = KafkaProtocolMessage(key, value, inputTopic, outputTopic, headers)
        if (messageMatcher.responseMatch(message) == null) {
          logger.error("no messageMatcher key for read message {}", message.key)
        } else {
          if (key == null || value == null) {
            logger.warn(" --- received message with null key or value")
          } else {
            logger.trace(" --- received {} {}", key, value)
          }
          val receivedTimestamp = clock.nowMillis
          val replyId           = messageMatcher.responseMatch(message)
          val messageKey        = if (key == null) "null" else new String(key)
          logMessage(s"Record received key=$messageKey", message)

          tracker ! MessageConsumed(
            replyId,
            receivedTimestamp,
            responseTransformer.map(_(message)).getOrElse(message),
          )
        }
      }
  }

  private def processorSupplier(
      tracker: ActorRef[TrackerMessage],
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      clock: Clock,
  ): KafkaMessageProcessorSupplier =
    new KafkaMessageProcessorSupplier(tracker, inputTopic, outputTopic, messageMatcher, responseTransformer, clock)

  def apply(
      streamsSettings: Map[String, AnyRef],
      actorSystem: ActorSystem,
      statsEngine: StatsEngine,
      clock: Clock,
  ): KafkaMessageTrackerPool = new KafkaMessageTrackerPool(streamsSettings, actorSystem, statsEngine, clock)
}

final class KafkaMessageTrackerPool(
    streamsSettings: Map[String, AnyRef],
    actorSystem: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaLogging with NameGen with KafkaSerdesImplicits {

  // Trackers map Output Topic (String) to Tracker/Actor
  private val trackers    = new ConcurrentHashMap[String, ActorRef[KafkaMessageTracker.TrackerMessage]]
  private val trackerName = "kafkaTracker"

  def tracker(
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): ActorRef[KafkaMessageTracker.TrackerMessage] = {
    trackers.computeIfAbsent(
      outputTopic,
      _ => {
        logger.debug("Computing new tracker for topic {}, there are currently {} other trackers", outputTopic, trackers.size())
        val name    = genName(trackerName)
        val tracker = actorSystem.actorOf(KafkaMessageTracker.actor(name, statsEngine, clock))
        val builder = new StreamsBuilder
        builder
          .stream[Array[Byte], Array[Byte]](outputTopic)
          .process(
            KafkaMessageTrackerPool.processorSupplier(
              tracker,
              inputTopic,
              outputTopic,
              messageMatcher,
              responseTransformer,
              clock,
            ),
          )

        val streams = new KafkaStreams(builder.build(), propertiesWithAppId(name))
        streams.cleanUp()
        streams.start()
        actorSystem.registerOnTermination {
          logger.debug("Closing stream {} with name {}", streams, name)
          streams.close()
        }
        tracker
      },
    )
  }

  private def propertiesWithAppId(appId: String) = {
    val props = new Properties()
    props.putAll(streamsSettings.asJava)
    props.put(APPLICATION_ID_CONFIG, appId)
    props
  }
}
