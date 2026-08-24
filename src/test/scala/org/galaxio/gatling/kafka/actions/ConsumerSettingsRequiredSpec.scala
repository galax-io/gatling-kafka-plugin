package org.galaxio.gatling.kafka.actions

import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serdes
import org.galaxio.gatling.kafka.client.KafkaSender
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt

/** Issue #227 — a request-reply with no consumer configuration is refused before the run starts, and produce-only is not.
  *
  * The two halves are one decision. Without consumer settings there is no reply channel, so every request-reply issued against
  * such a protocol used to be failed individually: one KO per virtual user, each carrying a near-zero interval, for a single
  * static misconfiguration nothing in the run could recover from. Thousands of samples that measured nothing, in the statistics
  * of the request the simulation declared.
  *
  * Whether the protocol carries consumer settings depends on no session data, so the answer is known before any user starts.
  * The precondition lives in the action's constructor, which `KafkaRequestReplyActionBuilder.build` runs while Gatling
  * materialises the scenario — so the simulation refuses to start instead of producing the samples.
  *
  * The second half bounds the first. Absent consumer configuration is not an error: it is the produce-only shape the DSL
  * offers, built by `KafkaProtocolBuilder.properties(...)`, whose own scaladoc directs readers to it — "Use `properties(...)`
  * for a produce-only protocol". `KafkaJavaapiMethodsGatlingTest`, three protocols in `KafkaGatlingTest` and all three
  * published `ProducerSimulation` examples are built that way. Publishing reaches `KafkaRequestActionBuilder` and never asks
  * for a tracker pool, so the refusal must not touch it — and this suite is what stops the refusal being widened onto it later.
  *
  * No broker: the precondition runs before anything is acquired or sent, and the produce-only case needs only a sender that
  * records the call.
  */
class ConsumerSettingsRequiredSpec extends munit.FunSuite {

  private final class StubClock(now: Long) extends Clock {
    override def nowMillis: Long = now
  }

  private final class RecordingAction(val name: String) extends Action {
    val lastSession: AtomicReference[Session]    = new AtomicReference[Session]()
    override def !(session: Session): Unit       = execute(session)
    override def execute(session: Session): Unit = lastSession.set(session)
  }

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
      requestName = _ => "request".success,
      producerTopic = _ => "request-topic".success,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  /** A protocol with producer settings and nothing else — what `KafkaProtocolBuilder.properties(...)` produces. */
  private def produceOnlyComponents(coreComponents: CoreComponents, sender: KafkaSender): KafkaComponents =
    KafkaComponents(
      coreComponents,
      KafkaProtocol(
        producerProperties = Map("bootstrap.servers" -> "localhost:9092"),
        consumerProperties = Map.empty,
        timeout = 5.seconds,
        messageMatcher = KafkaKeyMatcher,
      ),
      // What `getOrCreateTrackerPool` returns when the consumer properties carry no bootstrap servers.
      trackersPool = None,
      sender = sender,
    )

  private def withComponents(body: (CoreComponents, RecordingSender, RecordingAction, RecordingStatsEngine) => Unit): Unit = {
    val actorSystem = new ActorSystem()
    try {
      val statsEngine    = new RecordingStatsEngine
      val sender         = new RecordingSender
      val next           = new RecordingAction("next")
      val coreComponents =
        new CoreComponents(
          actorSystem,
          null,
          null,
          None,
          statsEngine,
          new StubClock(1_000L),
          null,
          GatlingConfiguration.loadForTest(),
        )
      body(coreComponents, sender, next, statsEngine)
    } finally actorSystem.close()
  }

  test("a request-reply built against a protocol with no consumer settings is refused, not run") {
    withComponents { (coreComponents, sender, next, statsEngine) =>
      val error = intercept[IllegalArgumentException] {
        new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
          produceOnlyComponents(coreComponents, sender),
          attributes,
          coreComponents,
          next,
          None,
        )
      }

      // Pinned whole, not by `contains`. A substring check cannot see a prefix, and `require` adds one:
      // it throws IllegalArgumentException("requirement failed: " + msg), which would put four meaningless
      // words in front of wording chosen to be read. That is why the guard is a `getOrElse(throw …)`.
      assertEquals(
        error.getMessage,
        KafkaRequestFailureMessages.consumerSettingsRequired,
        "the refusal must reach the operator exactly as written, with nothing prepended",
      )
      // And it must name the entry the gate actually reads. The pool is absent iff the consumer settings
      // carry no bootstrap.servers, so a protocol that did call consumeSettings reaches this too.
      assert(
        error.getMessage.contains("bootstrap.servers"),
        s"the refusal must name the missing entry, not just the DSL call: ${error.getMessage}",
      )
      assertEquals(
        sender.sends.get(),
        0,
        "nothing may be published by a request-reply that cannot exist",
      )
    }
  }

  test("the refusal happens at construction, so no virtual user ever sees it") {
    withComponents { (coreComponents, sender, next, statsEngine) =>
      intercept[IllegalArgumentException] {
        new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
          produceOnlyComponents(coreComponents, sender),
          attributes,
          coreComponents,
          next,
          None,
        )
      }

      // The distinction this feature turns on. Constructing is what `KafkaRequestReplyActionBuilder.build`
      // does while Gatling materialises the scenario, before any user is injected — so a refusal here is a
      // simulation that does not start, not a run that reports one failure per request. `next` never being
      // reached is the observable form of that: no user was advanced, because no user existed yet.
      assert(next.lastSession.get() == null, "no virtual user may be advanced by a refusal that precedes the run")
      assertEquals(
        statsEngine.responses.get().size,
        0,
        "and nothing may be reported: the point of refusing here is that the run produces no request rows at all",
      )
    }
  }

  test("a protocol that calls consumeSettings without bootstrap.servers is refused too, and told which entry is missing") {
    withComponents { (coreComponents, sender, next, statsEngine) =>
      // `consumeSettings("group.id" -> …)` compiles and yields no tracker pool, because the pool is keyed
      // on bootstrap.servers alone. The old message told this user to add consumeSettings, which they had
      // already done — the defect `KafkaRequestFailureMessages.remedyFor` exists to prevent one layer over.
      val components = KafkaComponents(
        coreComponents,
        KafkaProtocol(
          producerProperties = Map("bootstrap.servers" -> "localhost:9092"),
          consumerProperties = Map("group.id" -> "gatling"),
          timeout = 5.seconds,
          messageMatcher = KafkaKeyMatcher,
        ),
        trackersPool = None,
        sender = sender,
      )

      val error = intercept[IllegalArgumentException] {
        new KafkaRequestReplyAction[Array[Byte], Array[Byte]](components, attributes, coreComponents, next, None)
      }

      assert(
        error.getMessage.contains("bootstrap.servers"),
        s"the refusal must name the entry that is missing: ${error.getMessage}",
      )
      assert(
        error.getMessage.contains("if you already call consumeSettings"),
        s"and must not read as advice this user has already followed: ${error.getMessage}",
      )
    }
  }

  test("a produce-only action against the same protocol is built and publishes normally") {
    withComponents { (coreComponents, sender, next, statsEngine) =>
      // The bound on the refusal above. `properties(...)` is the documented produce-only shape and carries
      // no consumer settings by construction; publishing never asks for a reply channel, so it reaches
      // KafkaRequestActionBuilder and never touches the tracker pool. If this ever starts throwing, the
      // refusal has been widened onto a path it must not reach — and three published examples with it.
      val action = new KafkaRequestAction[Array[Byte], Array[Byte]](
        produceOnlyComponents(coreComponents, sender),
        attributes,
        coreComponents,
        next,
        None,
      )

      action.sendKafkaMessage(
        "request",
        KafkaProtocolMessage(
          key = "k".getBytes,
          value = "body".getBytes,
          producerTopic = "request-topic",
          consumerTopic = "request-topic",
        ),
        Session("scenario", 1L, null),
      )

      assertEquals(sender.sends.get(), 1, "a produce-only request must reach the producer with no consumer settings at all")
    }
  }
}
