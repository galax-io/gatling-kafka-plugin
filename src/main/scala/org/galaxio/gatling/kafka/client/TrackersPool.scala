package org.galaxio.gatling.kafka.client

import akka.actor.{ActorSystem, CoordinatedShutdown}
import io.gatling.commons.util.Clock
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.MessageConsumed
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object TrackersPool {
  private val ConsumerStartupTimeout = 30.seconds

  private[client] def awaitRunning(
      currentState: () => KafkaStreams.State,
      registerListener: KafkaStreams.StateListener => Unit,
      start: () => Unit,
      timeoutMillis: Long,
  ): Unit = {
    if (currentState() == KafkaStreams.State.RUNNING) {
      ()
    } else {
      val readyLatch = new CountDownLatch(1)
      registerListener { (newState, _) =>
        if (newState == KafkaStreams.State.RUNNING) {
          readyLatch.countDown()
        }
      }
      start()

      if (currentState() != KafkaStreams.State.RUNNING && !readyLatch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException(s"Reply consumer did not reach RUNNING state within ${timeoutMillis} ms")
      }
    }
  }
}

class TrackersPool(
    streamsSettings: Map[String, AnyRef],
    system: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaTrackerProvider with KafkaLogging with NameGen {

  private val trackers = new ConcurrentHashMap[String, KafkaMessageTracker]
  private val props    = new java.util.Properties()
  props.putAll(streamsSettings.asJava)

  override def tracker(
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): KafkaMessageTracker =
    trackers.computeIfAbsent(
      outputTopic,
      _ => {
        val actor =
          system.actorOf(KafkaMessageTrackerActor.props(statsEngine, clock), genName("kafkaTrackerActor"))

        val builder = new StreamsBuilder

        builder.stream[Array[Byte], Array[Byte]](outputTopic).foreach { case (k, v) =>
          val message = KafkaProtocolMessage(k, v, inputTopic, outputTopic)
          if (messageMatcher.responseMatch(message) == null) {
            logger.error(s"no messageMatcher key for read message")
          } else {
            if (k == null || v == null)
              logger.info(s" --- received message with null key or value")
            else
              logger.info(s" --- received  ${new String(k)} ${new String(v)}")
            val receivedTimestamp = clock.nowMillis
            val replyId           = messageMatcher.responseMatch(message)
            if (k != null)
              logMessage(
                s"Record received key=${new String(k)}",
                message,
              )
            else
              logMessage(
                s"Record received key=null",
                message,
              )

            actor ! MessageConsumed(
              replyId,
              receivedTimestamp,
              responseTransformer.map(_(message)).getOrElse(message),
            )
          }
        }

        val streams = new KafkaStreams(builder.build(), props)

        TrackersPool.awaitRunning(
          () => streams.state(),
          streams.setStateListener,
          () => {
            streams.cleanUp()
            streams.start()
          },
          TrackersPool.ConsumerStartupTimeout.toMillis,
        )

        CoordinatedShutdown(system).addJvmShutdownHook(streams.close())

        new KafkaMessageTracker(actor)
      },
    )
}
