package org.galaxio.gatling.kafka.client

import akka.actor.{ActorSystem, CoordinatedShutdown}
import io.gatling.commons.util.Clock
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.MessageConsumed
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object TrackersPool {
  private val ConsumerStartupTimeout = 30.seconds

  private[client] final case class TrackerCacheKey(
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  )

  private[client] def responseMatchIdOrError(
      message: KafkaProtocolMessage,
      messageMatcher: KafkaMatcher,
  ): Either[String, Array[Byte]] =
    Option(messageMatcher.responseMatch(message)).toRight("response matcher returned null match id")

  private[client] def consumedMessage(
      key: Array[Byte],
      value: Array[Byte],
      inputTopic: String,
      outputTopic: String,
      headers: org.apache.kafka.common.header.Headers,
  ): KafkaProtocolMessage =
    KafkaProtocolMessage(key, value, inputTopic, outputTopic, Option(headers))

  private[client] def trackerApplicationId(baseApplicationId: String, trackerKey: TrackerCacheKey): String = {
    val fingerprint = UUID.nameUUIDFromBytes(
      s"${trackerKey.inputTopic}|${trackerKey.outputTopic}|${System.identityHashCode(trackerKey.messageMatcher)}|${trackerKey.responseTransformer
          .fold("none")(transformer => System.identityHashCode(transformer).toString)}"
        .getBytes(StandardCharsets.UTF_8),
    )
    s"$baseApplicationId-$fingerprint"
  }

  private[client] def consumerProperties(
      consumerSettings: Map[String, AnyRef],
      trackerKey: TrackerCacheKey,
  ): Properties = {
    val props       = new Properties()
    props.putAll(consumerSettings.asJava)
    val baseGroupId =
      consumerSettings.getOrElse(ConsumerConfig.GROUP_ID_CONFIG, "gatling-test").toString
    props.put(ConsumerConfig.GROUP_ID_CONFIG, trackerApplicationId(baseGroupId, trackerKey))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    if (!consumerSettings.contains(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    }
    props
  }
}

class TrackersPool(
    consumerSettings: Map[String, AnyRef],
    system: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaTrackerProvider with KafkaLogging with NameGen {

  private val trackers = new ConcurrentHashMap[TrackersPool.TrackerCacheKey, KafkaMessageTracker]

  override def tracker(
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): KafkaMessageTracker =
    trackers.computeIfAbsent(
      TrackersPool.TrackerCacheKey(inputTopic, outputTopic, messageMatcher, responseTransformer),
      trackerKey => {
        val actor =
          system.actorOf(KafkaMessageTrackerActor.props(statsEngine, clock), genName("kafkaTrackerActor"))

        val props    = TrackersPool.consumerProperties(consumerSettings, trackerKey)
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
        consumer.subscribe(Collections.singletonList(outputTopic))

        val shutdown   = new AtomicBoolean(false)
        val readyLatch = new CountDownLatch(1)

        val thread = new Thread(
          () => {
            try {
              while (!shutdown.get()) {
                val records = consumer.poll(Duration.ofMillis(100))
                readyLatch.countDown()
                records.forEach { record =>
                  val message = TrackersPool.consumedMessage(
                    record.key(),
                    record.value(),
                    inputTopic,
                    outputTopic,
                    record.headers(),
                  )
                  TrackersPool.responseMatchIdOrError(message, messageMatcher) match {
                    case Left(_)        =>
                      logger.error(s"no messageMatcher key for read message")
                    case Right(replyId) =>
                      val receivedTimestamp = clock.nowMillis
                      if (logger.underlying.isDebugEnabled) {
                        if (record.key() != null)
                          logMessage(
                            s"Record received key=${new String(record.key())}",
                            message,
                          )
                        else
                          logMessage(
                            s"Record received key=null",
                            message,
                          )
                      }

                      actor ! MessageConsumed(
                        replyId,
                        receivedTimestamp,
                        responseTransformer.map(_(message)).getOrElse(message),
                      )
                  }
                }
              }
            } finally {
              consumer.close()
            }
          },
          s"kafka-tracker-${TrackersPool.trackerApplicationId("poll", trackerKey)}",
        )
        thread.setDaemon(true)
        thread.start()

        if (!readyLatch.await(TrackersPool.ConsumerStartupTimeout.toMillis, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException(
            s"Reply consumer did not start within ${TrackersPool.ConsumerStartupTimeout.toMillis} ms",
          )
        }

        CoordinatedShutdown(system).addJvmShutdownHook {
          shutdown.set(true)
          consumer.wakeup()
        }

        new KafkaMessageTracker(actor)
      },
    )
}
