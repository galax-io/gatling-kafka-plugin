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
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Issue #254 — a request the producer refuses to deliver must be reported with the kind of failure it was, not only its text.
  *
  * The kind is what separates "the broker rejected this record" from "the client was misconfigured" when reading a run
  * afterwards. It used to be handed to Gatling's response-code argument, which every OSS writer discards before anything is
  * written, so it reached no report; it now travels in the message.
  *
  * Against a real broker on purpose. The delivery-failure path only runs once the reply channel has actually been acquired, so
  * a stubbed sender cannot reach it — the send sits inside the acquisition continuation. `max.request.size` is set below the
  * record size to make the producer reject it deterministically, which surfaces as `RecordTooLargeException` through the
  * delivery callback rather than as a broker-side error, so no fault injection is needed.
  */
class DeliveryFailureReportingSpec extends munit.FunSuite with TestContainerForAll with StrictLogging {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  private val RequestTopic = "delivery-failure-request"
  private val ReplyTopic   = "delivery-failure-reply"

  /** Has to outlast acquiring the reply channel, which is what the request waits on before it is even sent. */
  private val ReplyTimeout: FiniteDuration = 30.seconds

  /** Small enough that the record below cannot fit, large enough that the producer still starts. */
  private val MaxRequestSize = 1024

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.MAX_REQUEST_SIZE_CONFIG       -> MaxRequestSize.toString,
  )

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
      // Never resolved here: the action is driven through `sendKafkaMessage` with a pre-built message.
      producerTopic = _ => "unused".success,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  test("a record the producer refuses to deliver is reported with the kind of failure it was") {
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
          messageMatcher = KafkaKeyMatcher,
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

        val oversized = KafkaProtocolMessage(
          "too-large".getBytes,
          Array.fill(MaxRequestSize * 4)('x'.toByte),
          RequestTopic,
          ReplyTopic,
        )
        action.sendKafkaMessage("request-reply", oversized, Session("scenario", 1L, null))

        // The failure is reported from the producer's callback, which runs once acquisition has handed the
        // tracker over, so it is not synchronous with the call above.
        val deadline = System.currentTimeMillis() + ReplyTimeout.toMillis + 60000
        while (statsEngine.responses.get().isEmpty && System.currentTimeMillis() < deadline)
          Thread.sleep(100)

        val responses = statsEngine.responses.get()
        assertEquals(responses.size, 1, "a record that could not be delivered must still get exactly one outcome")
        assertEquals(responses.head.status, (KO: Status))

        val message = responses.head.message.getOrElse("")
        assert(
          message.startsWith("RecordTooLargeException: "),
          s"the report must name the kind of failure, which is the half Gatling's response-code slot drops: $message",
        )
        assert(
          message.length > "RecordTooLargeException: ".length,
          s"and must keep the producer's own text alongside it: $message",
        )
        assertEquals(next.completed, 1, "the virtual user must be advanced exactly once")
      } finally {
        sender.close()
        actorSystem.close()
      }
    }
  }
}
