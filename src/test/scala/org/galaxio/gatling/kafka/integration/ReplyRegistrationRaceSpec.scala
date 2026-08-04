package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.ConfluentKafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import io.gatling.commons.stats.{OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.{LoggedResponse, RecordingStatsEngine}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Serdes}
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyAction
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Regression coverage for issue #191 — a reply must never be dropped because it arrived before its own request finished
  * registering.
  *
  * The defect was an ordering one, not a concurrency one. The pending record used to be created from the producer's
  * acknowledgement callback, i.e. *after* the request was already on the wire and answerable. A responder that answers in under
  * a millisecond can therefore have its reply polled and delivered before that callback has run, at which point the reply
  * matches nothing, is discarded silently, and the request fails on its reply timeout — indistinguishable from a system under
  * test that never answered.
  *
  * This forces the condition rather than waiting for it: the responder echoes immediately, the reply timeout is generous, and
  * enough requests are issued that the window is hit repeatedly. Every request is answered, so any reply-timeout failure is
  * this bug and nothing else.
  */
class ReplyRegistrationRaceSpec extends munit.FunSuite with TestContainerForAll {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  private val RequestTopic = "race-request"
  private val ReplyTopic   = "race-reply"

  /** Generous on purpose: the point is that nothing times out, so a timeout means a lost reply rather than a slow broker. */
  private val ReplyTimeout: FiniteDuration = 30.seconds

  private val Requests = 60

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
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
      producerTopic = None,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  test("every reply is matched, even when it arrives before its own request has finished registering") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      createTopics(bootstrap, RequestTopic, ReplyTopic)

      val actorSystem = new ActorSystem()
      val statsEngine = new RecordingStatsEngine
      val clock       = new SystemClock
      val sender      = KafkaSender(producerSettings(bootstrap))
      val next        = new CountingAction

      // Echoes key and value straight back, with no delay at all. That immediacy is the whole point: a
      // responder that answered slowly would never reach the window this test exists to cover.
      val echoSender      = KafkaSender(producerSettings(bootstrap))
      val echoReady       = new CountDownLatch(1)
      val responder       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap) + (ConsumerConfig.GROUP_ID_CONFIG -> "race-responder"),
        Set(RequestTopic),
        record => {
          echoReady.countDown()
          echoSender.send(KafkaProtocolMessage(record.key(), record.value(), ReplyTopic, ReplyTopic))(
            _ => (),
            error => println(s"[race responder] failed to echo: $error"),
          )
        },
        error => println(s"[race responder] consumer failed: $error"),
      )
      val responderThread = new Thread(responder, "race-responder")
      responderThread.setDaemon(true)
      responderThread.start()

      try {
        val pool = new KafkaMessageTrackerPool(consumerSettings(bootstrap), actorSystem, statsEngine, clock)

        val protocol = KafkaProtocol(
          producerProperties = producerSettings(bootstrap),
          consumerProperties = consumerSettings(bootstrap),
          timeout = ReplyTimeout,
          messageMatcher = KafkaKeyMatcher,
        )

        val coreComponents =
          new CoreComponents(actorSystem, null, null, None, statsEngine, clock, null, GatlingConfiguration.loadForTest())

        val action = new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
          KafkaComponents(coreComponents, protocol, Some(pool), sender),
          attributes,
          coreComponents,
          next,
          None,
        )

        // Wait for the responder to be positioned before measuring, so a skipped first echo is not
        // mistaken for a dropped reply.
        val warmup = KafkaProtocolMessage("warmup".getBytes, "warmup".getBytes, RequestTopic, ReplyTopic)
        val sent   = new CountDownLatch(1)
        sender.send(warmup)(_ => sent.countDown(), _ => sent.countDown())
        assert(sent.await(30, TimeUnit.SECONDS), "warmup request was never delivered")
        assert(echoReady.await(60, TimeUnit.SECONDS), "responder never received anything")

        (1 to Requests).foreach { i =>
          action.sendKafkaMessage(
            "request-reply",
            KafkaProtocolMessage(s"race-$i".getBytes, s"body-$i".getBytes, RequestTopic, ReplyTopic),
            Session("scenario", i.toLong, null),
          )
        }

        val deadline = System.currentTimeMillis() + ReplyTimeout.toMillis + 60000
        while (statsEngine.responses.get().size < Requests && System.currentTimeMillis() < deadline)
          Thread.sleep(100)

        val responses: Vector[LoggedResponse] = statsEngine.responses.get()
        assertEquals(responses.size, Requests, s"only ${responses.size} of $Requests requests reported an outcome")

        val timedOut = responses.filter(_.message.exists(_.startsWith("Reply timeout")))
        assertEquals(
          timedOut.size,
          0,
          s"${timedOut.size} of $Requests replies were lost: the responder answered every request, so a reply " +
            "timeout here means the reply arrived before its request had registered and was discarded",
        )
        assert(
          responses.forall(_.status == (OK: Status)),
          s"unexpected failures: ${responses.filterNot(_.status == (OK: Status)).map(_.message)}",
        )
        assertEquals(next.completed, Requests, "every virtual user must be advanced exactly once")
      } finally {
        responder.close()
        echoSender.close()
        sender.close()
        actorSystem.close()
      }
    }
  }
}
