package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.{ActorRef, ActorSystem}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{KafkaMessage, MessageConsumed}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class KafkaMessageTrackerPool(
    streamsSettings: Map[String, AnyRef],
    system: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaLogging with NameGen {

  // Trackers map Output Topic (String) to Tracker/Actor
  private val trackers = new ConcurrentHashMap[String, ActorRef[KafkaMessageTracker.KafkaMessage]]
  private val props    = new java.util.Properties()
  props.putAll(streamsSettings.asJava)

  def tracker(
      topic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): ActorRef[KafkaMessageTracker.KafkaMessage] =
    trackers.computeIfAbsent(
      topic,
      _ => {
        val tracker = system.actorOf(KafkaMessageTracker.actor(genName("kafkaTrackerActor"), statsEngine, clock))
        val builder = new StreamsBuilder
        builder
          .stream[Array[Byte], Array[Byte]](topic)
          .process(
            new GatlingReporting(
              tracker,
              topic,
              messageMatcher,
              responseTransformer,
              clock,
            ),
          )

        val streams = new KafkaStreams(builder.build(), props)
        streams.cleanUp()
        streams.start()
        system.registerOnTermination(streams.close())

        tracker
      },
    )
}

/** Because we may need access to headers whilst deserializing (which can contain deserialization info), we will need to use the
  * Kafka Processor Api
  * https://docs.confluent.io/platform/current/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-process
  */
class GatlingReporting(
    tracker: ActorRef[KafkaMessage],
    topic: String,
    messageMatcher: KafkaMatcher,
    responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
    clock: Clock,
) extends ProcessorSupplier[Array[Byte], Array[Byte], Void, Void] with KafkaLogging {

  override def get(): Processor[Array[Byte], Array[Byte], Void, Void] = new ReportingProcessor

  private class ReportingProcessor extends Processor[Array[Byte], Array[Byte], Void, Void] {

    override def process(record: api.Record[Array[Byte], Array[Byte]]): Unit = {
      val headers = record.headers()
      val key     = record.key()
      val value   = record.value()
      val message = KafkaProtocolMessage(key, value, topic, Option(headers))
      if (messageMatcher.responseMatch(message) == null) {
        logger.error("no messageMatcher key for read message {}", message.key)
      } else {
        if (key == null || value == null) {
          logger.warn(" --- received message with null key or value")
        } else {
          logger.debug(" --- received {} {}", new String(key), new String(value))
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
}
