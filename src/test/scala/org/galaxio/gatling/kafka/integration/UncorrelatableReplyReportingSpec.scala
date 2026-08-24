package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.ConfluentKafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{KO, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Serdes}
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyAction
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaValueMatcher
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Issue #228, end to end — a request whose reply arrived in a shape the matcher cannot read must not be reported as one that
  * nobody answered.
  *
  * The distinction is the whole feature, and it lives in the failure text: both cases are a reply timeout, and both span the
  * whole reply budget. Gatling's assertion API cannot read a failure message, so `KafkaFailureModesGatlingTest` can only prove
  * the half that is expressible — that header correlation reaches the request and value correlation does not. Removing the
  * tracker's diagnosis entirely leaves that simulation green. This spec is what closes that: it reads the reported message.
  *
  * Against a real broker rather than the tracker in isolation. `KafkaMessageTrackerSpec` pins the wording by handing the actor
  * a hand-built `MessageConsumed`; what it cannot show is that a genuine tombstone on a genuine topic reaches that branch at
  * all — that the consumer polls it, that `KafkaProtocolMessage.from` carries the null value through, and that the matcher
  * returns null for it. Those are the steps between a service answering and the plugin noticing, and they are exactly the ones
  * a mock reproduces incorrectly.
  *
  * The rig mirrors `DeliveryFailureReportingSpec`: one container, the action driven through `sendKafkaMessage` with a pre-built
  * message. Nothing consumes the request topic, so the only record on the reply topic is the tombstone published below — which
  * is the shape a compacted topic produces and a service answering "this entity is gone" sends.
  */
class UncorrelatableReplyReportingSpec extends munit.FunSuite with TestContainerForAll with StrictLogging {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  private val RequestTopic = "uncorrelatable-request"
  private val ReplyTopic   = "uncorrelatable-reply"

  /** Long enough to outlast acquiring the reply channel and to let the tombstone be polled before the scan fires, short enough
    * that the test does not dominate the suite. The scan runs at one-second granularity, so the reported span is this or a
    * little more.
    */
  private val ReplyTimeout: FiniteDuration = 20.seconds

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  /** `earliest`, so the tombstone is read whether it lands before or after the reply channel finishes subscribing. The race is
    * not what this spec is about, and leaving it in would make the assertion intermittent.
    */
  private def consumerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
  )

  private def createTopics(bootstrap: String, names: String*): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap).asJava,
    )
    try admin.createTopics(names.map(new NewTopic(_, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
    finally admin.close()
  }

  private final class SystemClock extends Clock {
    override def nowMillis: Long = System.currentTimeMillis()
  }

  private final class CountingAction extends Action {
    private val count = new AtomicInteger(0)

    override def name: String                    = "counting-next"
    override def !(session: Session): Unit       = execute(session)
    override def execute(session: Session): Unit = { count.incrementAndGet(); () }

    def completed: Int = count.get()
  }

  private def attributes: KafkaAttributes[Array[Byte], Array[Byte]] =
    KafkaAttributes[Array[Byte], Array[Byte]](
      requestName = _ => "request-reply".success,
      producerTopic = _ => "unused".success,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  private def publishTombstone(sender: KafkaSender): Unit = {
    val latch = new java.util.concurrent.CountDownLatch(1)
    sender.send(
      // A key and no value: what Kafka calls a tombstone, and what `matchByValue` can derive nothing from.
      KafkaProtocolMessage("gone".getBytes, null, ReplyTopic, ReplyTopic),
    )(_ => latch.countDown(), _ => latch.countDown())
    latch.await(30, TimeUnit.SECONDS)
    ()
  }

  test("a reply that arrived and could not be correlated is named in the timeout it causes") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      createTopics(bootstrap, RequestTopic, ReplyTopic)

      val actorSystem = new ActorSystem()
      val statsEngine = new RecordingStatsEngine
      val clock       = new SystemClock
      val sender      = KafkaSender(producerSettings(bootstrap))
      val next        = new CountingAction

      try {
        val pool           = new KafkaMessageTrackerPool(consumerSettings(bootstrap), actorSystem, statsEngine, clock)
        val protocol       = KafkaProtocol(
          producerProperties = producerSettings(bootstrap),
          consumerProperties = consumerSettings(bootstrap),
          timeout = ReplyTimeout,
          // The matcher the issue is about: it reads the payload, and a tombstone has none.
          messageMatcher = KafkaValueMatcher,
        )
        val coreComponents =
          new CoreComponents(actorSystem, null, null, None, statsEngine, clock, null, GatlingConfiguration.loadForTest())
        val action         = new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
          KafkaComponents(coreComponents, protocol, Some(pool), sender),
          attributes,
          coreComponents,
          next,
          None,
        )

        // A request with a payload, so the matcher derives an id from it and the record is registered.
        val request = KafkaProtocolMessage("req".getBytes, "correlate-me".getBytes, RequestTopic, ReplyTopic)
        action.sendKafkaMessage("request-reply", request, Session("scenario", 1L, null))

        // The service answers — with a tombstone. Nothing consumes RequestTopic, so this is the only record
        // the reply channel will ever see, and it is one it cannot place.
        publishTombstone(sender)

        val deadline = System.currentTimeMillis() + ReplyTimeout.toMillis + 90000
        while (statsEngine.responses.get().isEmpty && System.currentTimeMillis() < deadline)
          Thread.sleep(100)

        val responses = statsEngine.responses.get()
        assertEquals(responses.size, 1, "an unanswerable request must still get exactly one outcome")
        assertEquals(responses.head.status, (KO: Status))

        val message = responses.head.message.getOrElse("")
        // The timeout is still reported: it did happen.
        assert(
          message.startsWith(s"Reply timeout after ${ReplyTimeout.toMillis} ms"),
          s"the timeout itself must still be reported: $message",
        )
        // And this is the clause the whole feature exists for. Remove the tracker's uncorrelatable-reply
        // tracking and this is the assertion that goes red — which is what KafkaFailureModesGatlingTest
        // cannot do, because Gatling's assertion API cannot read a failure message.
        assert(
          message.contains("could not read a correlation id from"),
          s"a reply arrived that could not be placed, and the timeout must say so: $message",
        )
        assert(
          message.contains("KafkaValueMatcher"),
          s"and must name the matcher that could not read it: $message",
        )
        // The remedy is derived from that matcher, never fixed — advice hardcoded at "correlate on a key or
        // a header" was handed to readers already doing exactly that.
        assert(
          message.contains("the payload cannot be null"),
          s"and the remedy must be the one for value matching: $message",
        )
        assertEquals(next.completed, 1, "the virtual user must be advanced exactly once")
      } finally {
        sender.close()
        actorSystem.close()
      }
    }
  }
}
