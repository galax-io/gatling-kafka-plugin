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
import scala.concurrent.duration.{DurationInt, FiniteDuration}

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

  /** Returns `start` on its first reading and `end` on every one after it.
    *
    * The action reads the clock exactly twice per rejected request — once on entry for the start instant, once when it reports
    * — so this pins the reported interval without depending on wall-clock time. Only the action gets one; the pool keeps a
    * fixed clock, or its idle sweep would consume readings this is counting.
    */
  private final class SteppingClock(start: Long, end: Long) extends Clock {
    private val readings         = new AtomicInteger(0)
    override def nowMillis: Long = if (readings.getAndIncrement() == 0) start else end
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
      shutDownPoolFirst: Boolean = false,
      actionClock: Clock = new StubClock(1_000L),
      timeout: FiniteDuration = 5.seconds,
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
        timeout = timeout,
        messageMatcher = matcher,
      )
      // The action's own clock, deliberately not the pool's: the pool's idle sweep reads its clock on a
      // schedule, which would consume the readings a SteppingClock is counting.
      val coreComponents =
        new CoreComponents(actorSystem, null, null, None, statsEngine, actionClock, null, GatlingConfiguration.loadForTest())
      val action         = new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
        KafkaComponents(coreComponents, protocol, Some(pool), sender),
        attributes,
        coreComponents,
        next,
        None,
      )
      // Closing the ActorSystem runs the pool's termination hook on this thread, which shuts down the
      // executors acquisition needs; from then on `acquireTracker` reports failure on the calling
      // thread, which is what makes the acquisition-failure path observable here without a broker.
      // `close` is idempotent, so the `finally` below stays correct.
      if (shutDownPoolFirst) actorSystem.close()
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

  /** Correlatable under the default matcher, so it reaches the acquisition step instead of the guard above it. */
  private def keyedMessage: KafkaProtocolMessage =
    KafkaProtocolMessage(
      key = "id".getBytes,
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
      // Reported, not swallowed — the same guarantee as before, under a name that says what happened.
      //
      // It moved off "request-reply" because of what Gatling does with the reading: every request entry
      // updates the response-time digest for the name it carries, KO included, and the assertion API has
      // no successful-only scope for response time. A rejection left on the declared name therefore puts a
      // sample the system under test never produced into the percentile the simulation asserts on
      // (issue #227). The name is the only field a plugin controls that segregates samples — the
      // response-code slot is discarded before a run's data is written — so the name is what moves.
      assertEquals(
        responses.head.requestName,
        "request-reply [rejected: no correlation id]",
        "it must be reported against a name of its own, neither swallowed nor blended into the request's latency",
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

  test("a failure to acquire the reply channel is reported by its kind as well as its text") {
    // Issue #254: the kind used to travel in logResponse's response-code argument, which Gatling OSS
    // discards before writing simulation.log — so it reached no report at all. It belongs in the
    // message, which is what the errors table and the console summary actually show.
    withAction(KafkaKeyMatcher, shutDownPoolFirst = true) { (action, sender, statsEngine, next) =>
      action.sendKafkaMessage("request-reply", keyedMessage, Session("scenario", 1L, null))

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "a request whose reply channel cannot be acquired must still get an outcome")
      assertEquals(responses.head.status, (KO: Status))

      // Pinned to the whole message, not just the prefix: the pool raises IllegalStateException from two
      // unrelated guards, so a prefix-only assertion would stay green while covering the other one, and a
      // length check would be satisfied by a refactor that replaced the text instead of prefixing it.
      val message = responses.head.message.getOrElse("")
      assert(
        message.startsWith("IllegalStateException: "),
        s"the failure must name its kind, not just its text: $message",
      )
      assert(
        message.contains("Tracker pool is shutting down"),
        s"and must keep the original text alongside it, from the guard this test drives: $message",
      )

      assertEquals(sender.sends.get(), 0, "nothing was published, so nothing must reach the producer")
      assert(next.lastSession.get() != null, "the virtual user must be advanced rather than left hanging")
      assert(next.lastSession.get().isFailed, "and advanced as failed")
    }
  }

  test("a request whose reply channel cannot be acquired is reported under its own name, with the wait it really took") {
    // The other rejection, and the one that matters most to a percentile. The two keyless cases above
    // report near-zero intervals, which drag a percentile down; this one reports the whole wait before it
    // gave up, which drags it up. Both describe a request that never reached the broker, so both move off
    // the declared name — and this one has to keep its measured wait when it moves. Reporting it as zero to
    // make rejections uniform would trade one wrong number for another (issue #227).
    //
    // The clock is what pins the interval: the action reads it exactly twice on this path, on entry and
    // when it reports, so the reported span is 3_500 ms whatever the wall clock did.
    withAction(KafkaKeyMatcher, shutDownPoolFirst = true, actionClock = new SteppingClock(1_000L, 4_500L)) {
      (action, sender, statsEngine, next) =>
        action.sendKafkaMessage("request-reply", keyedMessage, Session("scenario", 1L, null))

        val responses = statsEngine.responses.get()
        assertEquals(responses.size, 1, "a request whose reply channel never arrived must still get an outcome")
        assertEquals(responses.head.status, (KO: Status))
        assertEquals(
          responses.head.requestName,
          "request-reply [rejected: no reply channel]",
          "it never reached the broker, so it must not report against the request's own name",
        )
        assertEquals(
          responses.head.endTimestamp - responses.head.startTimestamp,
          3_500L,
          "the wait is relocated, not flattened: a rejection that waited must report what it waited",
        )
        assertEquals(
          sender.sends.get(),
          0,
          "nothing is published when the channel that would carry its reply could not be established",
        )
        assert(next.lastSession.get() != null, "the virtual user must be advanced rather than left hanging")
        assert(next.lastSession.get().isFailed, "and advanced as failed")
    }
  }

  test("an acquisition that times out is reported from the pool's own thread, under the derived name") {
    // The asynchronous half of the reply-channel rejection, and the one RejectionKind.NoReplyChannel's
    // scaladoc is actually about: "the whole consumer-assignment wait", the rejection whose interval
    // inflates rather than deflates a percentile.
    //
    // The sibling test above shuts the pool down first, which makes acquireTracker fail on the calling
    // thread through its early return. That is deterministic and cheap, but it never leaves the caller —
    // so it does not exercise setupExecutor.schedule -> readiness.completeExceptionally -> whenCompleteAsync
    // -> onFailure -> reportRejection, which is where the reporting actually happens in production, on a
    // different thread. A dead port plus a short assignment budget is the only rig that reaches it.
    withAction(KafkaKeyMatcher, timeout = 400.millis) { (action, sender, statsEngine, next) =>
      action.sendKafkaMessage("request-reply", keyedMessage, Session("scenario", 1L, null))

      val deadline = System.currentTimeMillis() + 30_000L
      while (statsEngine.responses.get().isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(50L)

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "the continuation must report exactly one outcome, from whatever thread it runs on")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(
        responses.head.requestName,
        "request-reply [rejected: no reply channel]",
        "the derived name must be applied on the asynchronous path too, not only the synchronous one",
      )
      assertEquals(sender.sends.get(), 0, "nothing is published when the channel never became available")
      assert(next.lastSession.get() != null, "the virtual user must be advanced from the pool's continuation thread")
      assert(next.lastSession.get().isFailed, "and advanced as failed")
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
