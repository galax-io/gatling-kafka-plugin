package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes}
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaValueMatcher}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt

/** Issue #167 — a request-reply that supplies nothing the configured matcher can correlate on must be failed before it is sent,
  * not sent and then mismatched.
  *
  * Under the default `matchByKey` a request with no key produced an empty correlation id, which every other keyless request
  * also produced. They shared one slot in the correlation table, so a reply resolved whichever request happened to occupy it:
  * one virtual user was credited with another's answer while the real owner timed out.
  *
  * No broker here on purpose. The guard runs before the reply channel is acquired and before the record reaches the producer,
  * so the interesting assertions are that the outcome is reported at all and that the sender is never called — both of which
  * are deterministic and need nothing running. The end-to-end behaviour against a real broker is `KeylessCorrelationSpec`.
  */
class KafkaRequestReplyActionSpec extends munit.FunSuite {

  private final class StubClock(now: Long) extends Clock {
    override def nowMillis: Long = now
  }

  private final class RecordingAction(val name: String) extends Action {
    val lastSession: AtomicReference[Session]    = new AtomicReference[Session]()
    override def !(session: Session): Unit       = execute(session)
    override def execute(session: Session): Unit = lastSession.set(session)
  }

  /** Records whether the record ever reached the producer. The whole point of failing early is that it must not. */
  private final class RecordingSender extends KafkaSender {
    val sends: AtomicInteger = new AtomicInteger(0)

    override def send(protocolMessage: KafkaProtocolMessage)(
        onSuccess: RecordMetadata => Unit,
        onFailure: Throwable => Unit,
    ): Unit = { sends.incrementAndGet(); () }

    override def close(): Unit = ()
  }

  private def attributes: KafkaAttributes[Array[Byte], Array[Byte]] =
    KafkaAttributes[Array[Byte], Array[Byte]](
      requestName = _ => "request-reply".success,
      // Never resolved by this suite: it drives the action through `sendKafkaMessage` with a
      // pre-built message rather than through `sendRequest`, which is what reads this.
      producerTopic = _ => "unused".success,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  /** Points at a port nothing listens on: the pool must be present for the request-reply path to be taken at all, but this
    * guard returns before the pool is touched, so it never has to work.
    */
  private def deadConsumerSettings: Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> "localhost:0",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
  )

  private def withAction(
      matcher: org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher,
  )(
      body: (KafkaRequestReplyAction[Array[Byte], Array[Byte]], RecordingSender, RecordingStatsEngine, RecordingAction) => Unit,
  ): Unit = {
    val actorSystem = new ActorSystem()
    val statsEngine = new RecordingStatsEngine
    val clock       = new StubClock(1_000L)
    val sender      = new RecordingSender
    val next        = new RecordingAction("next")
    try {
      val pool           = new KafkaMessageTrackerPool(deadConsumerSettings, actorSystem, statsEngine, clock)
      val protocol       = KafkaProtocol(
        producerProperties = Map.empty,
        consumerProperties = deadConsumerSettings,
        timeout = 5.seconds,
        messageMatcher = matcher,
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
      body(action, sender, statsEngine, next)
    } finally actorSystem.close()
  }

  private def keylessMessage: KafkaProtocolMessage =
    KafkaProtocolMessage(
      key = null,
      value = "body".getBytes,
      producerTopic = "request-topic",
      consumerTopic = "reply-topic",
    )

  test("a request with no correlation id is failed at issue time rather than sent") {
    withAction(KafkaKeyMatcher) { (action, sender, statsEngine, next) =>
      action.sendKafkaMessage("request-reply", keylessMessage, Session("scenario", 1L, null))

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "a request that cannot be correlated must still get an outcome")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(
        responses.head.requestName,
        "request-reply",
        "and it must be reported against the request, not swallowed",
      )
      assertEquals(
        sender.sends.get(),
        0,
        "it must not reach the producer: a request whose reply could never be matched must not be put on the wire",
      )
      assert(next.lastSession.get() != null, "the virtual user must be advanced rather than left hanging")
      assert(next.lastSession.get().isFailed, "and advanced as failed")
    }
  }

  test("the failure names the matcher and the remedy") {
    withAction(KafkaKeyMatcher) { (action, _, statsEngine, _) =>
      action.sendKafkaMessage("request-reply", keylessMessage, Session("scenario", 1L, null))

      val message = statsEngine.responses.get().head.message.getOrElse("")
      assert(message.contains("KafkaKeyMatcher"), s"unexpected message: $message")
      assert(message.contains("matchByValue"), s"unexpected message: $message")
      // Not the reused-id wording: the scenario never reused anything, it supplied nothing.
      assert(!message.contains("reused"), s"unexpected message: $message")
    }
  }

  test("a keyless request is not rejected when the matcher correlates on something it carries") {
    // The guard must key off what the matcher yields, not off the key: a keyless request under
    // matchByValue has a perfectly good correlation id.
    //
    // This asserts only that the request is NOT rejected, which is all this rig can see. Acquisition is
    // asynchronous and the pool points at a dead port, so the send can never complete here and
    // `sender.sends` stays 0 whatever the action does — asserting on it would test the rig, not the
    // code. That the request is actually published is covered end-to-end by KeylessCorrelationSpec's
    // "concurrent keyless requests still correlate when the matcher uses a field they carry".
    withAction(KafkaValueMatcher) { (action, _, statsEngine, _) =>
      action.sendKafkaMessage("request-reply", keylessMessage, Session("scenario", 1L, null))

      assertEquals(
        statsEngine.responses.get().count(_.message.exists(_.contains("supplies no value"))),
        0,
        "a request whose value the matcher can correlate on must not be rejected for having no key",
      )
    }
  }
}
