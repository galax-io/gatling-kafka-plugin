package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{ConsumerFailure, MessageConsumed, MessagePublished, SendFailed}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class KafkaMessageTrackerSpec extends munit.FunSuite {

  test("consumer failure fails pending request immediately with explicit error") {
    val statsEngine = new RecordingStatsEngine
    val clock       = new StubClock(2_000L)
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, clock, KafkaKeyMatcher, None)
    val behavior    = tracker.init()
    val session     = Session("scenario", 1L, null)

    behavior(
      MessagePublished(
        matchId = "match-1".getBytes(StandardCharsets.UTF_8),
        sentTimestamp = 1_000L,
        replyTimeout = 0L,
        checks = Nil,
        session = session,
        next = next,
        requestName = "request-reply",
      ),
    )
    behavior(ConsumerFailure("boom"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1)

    val response = responses.head
    assertEquals(response.requestName, "request-reply")
    assertEquals(response.status, KO)
    assertEquals(response.startTimestamp, 1_000L)
    assertEquals(response.endTimestamp, 2_000L)
    assertEquals(response.message, Some("Consumer failure: boom"))

    val nextSession = next.lastSession.get()
    assert(nextSession != null)
    assert(nextSession.isFailed)
  }

  test("a reply matching no pending request is discarded silently") {
    val statsEngine = new RecordingStatsEngine
    val clock       = new StubClock(2_000L)
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, clock, KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    // Nothing was ever published, so this reply correlates with no outstanding request. Holding reply
    // channels for the whole run makes this the common case — replies to already-completed requests
    // and third-party traffic keep arriving — so it must stay silent rather than becoming a failure.
    behavior(
      MessageConsumed(
        received = 2_000L,
        message = KafkaProtocolMessage(
          key = "unmatched".getBytes(StandardCharsets.UTF_8),
          value = "reply".getBytes(StandardCharsets.UTF_8),
          producerTopic = "reply-topic",
          consumerTopic = "reply-topic",
        ),
      ),
    )

    assertEquals(statsEngine.responses.get().size, 0, "an unmatched reply must not be reported")
    assert(next.lastSession.get() == null, "an unmatched reply must not advance any virtual user")
  }

  // Issue #166. These need a real ActorSystem rather than the direct `tracker.init()` style above: arming
  // the periodic scan touches `scheduler` and `self`, which only exist once the actor was created through
  // `actorSystem.actorOf`.
  private def withActorSystem(body: ActorSystem => Unit): Unit = {
    val actorSystem = new ActorSystem()
    try body(actorSystem)
    finally actorSystem.close()
  }

  test("a delivery failure for a superseded request does not fail the one that replaced it") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val released    = new AtomicInteger(0)
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()
    val matchId     = "match-race".getBytes(StandardCharsets.UTF_8)

    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L).copy(token = 1L))
    behavior(replyFor("match-race"))
    behavior(
      published(next, replyTimeout = 0L, requestStart = 7_000L)
        .copy(token = 2L, onComplete = () => { released.incrementAndGet(); () }),
    )
    behavior(SendFailed(matchId, "Broker unavailable", token = 1L))

    assertEquals(statsEngine.responses.get().size, 1, "a late failure for a completed request must not report B")
    assertEquals(released.get(), 0, "and must not release B's channel reference")
  }

  test("a match id reused while still in flight fails the displaced request instead of losing it") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val released    = new AtomicInteger(0)
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    // Two concurrent virtual users on the same constant key — which is what the repo's own example
    // simulations use. Before, the first record was overwritten silently: no success, no failure, no
    // continuation, and its channel reference never returned.
    behavior(
      published(next, replyTimeout = 0L, requestStart = 1_000L)
        .copy(token = 1L, onComplete = () => { released.incrementAndGet(); () }),
    )
    behavior(published(next, replyTimeout = 0L, requestStart = 2_000L).copy(token = 2L))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1, "the displaced request must be reported, not silently dropped")
    assertEquals(responses.head.status, (KO: Status))
    assert(
      responses.head.message.exists(_.contains("Match id reused")),
      s"unexpected message: ${responses.head.message}",
    )
    assertEquals(released.get(), 1, "and its channel reference must be released")
  }

  // Issue #191. Registration now happens before the send, so a reply and its own acknowledgement can
  // arrive in either order. These drive the behaviour directly, which is deterministic — the broker-level
  // race is covered by ReplyRegistrationRaceSpec.
  private def replyFor(key: String): MessageConsumed =
    MessageConsumed(
      received = 5_000L,
      message = KafkaProtocolMessage(
        key = key.getBytes(StandardCharsets.UTF_8),
        value = "reply".getBytes(StandardCharsets.UTF_8),
        producerTopic = "reply-topic",
        consumerTopic = "reply-topic",
      ),
    )

  private def published(next: Action, replyTimeout: Long, requestStart: Long): MessagePublished =
    MessagePublished(
      matchId = "match-race".getBytes(StandardCharsets.UTF_8),
      sentTimestamp = requestStart,
      replyTimeout = replyTimeout,
      checks = Nil,
      session = Session("scenario", 1L, null),
      next = next,
      requestName = "request-reply",
    )

  test("a reply is reported as soon as it arrives") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    // The order this feature exists for: registered first, answered second. Before the fix the request
    // was not registered until the producer acknowledged it, so a fast reply found nothing and was
    // dropped — the request then failed on its reply timeout with no sign a reply had ever arrived.
    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L))
    behavior(replyFor("match-race"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1, "a reply that arrived must be reported, never withheld pending another message")
    assertEquals(responses.head.status, (OK: Status))
    assertEquals(responses.head.startTimestamp, 1_000L, "measured from the handoff to the producer")
    assertEquals(responses.head.endTimestamp, 5_000L)
  }

  test("a request that is never answered times out, measured from its handoff") {
    // Needs an ActorSystem: a positive reply timeout arms the periodic scan, which touches `scheduler`.
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      val next        = new RecordingAction("next")
      val ref         = actorSystem.actorOf(
        KafkaMessageTracker
          .actor[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None),
      )

      // Registered at 1_000 with a 5 s timeout; the clock is at 9_000.
      ref ! published(next, replyTimeout = 5_000L, requestStart = 1_000L)
      ref ! KafkaMessageTracker.TimeoutScan
      Thread.sleep(500)

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "an unanswered request must time out")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(responses.head.startTimestamp, 1_000L, "measured from the handoff, not from channel acquisition")
      assertEquals(responses.head.message, Some("Reply timeout after 5000 ms"))
    }
  }

  test("a delivery failure removes the pending request, reports KO, and releases the channel once") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val released    = new AtomicInteger(0)
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(4_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    behavior(
      published(next, replyTimeout = 0L, requestStart = 1_000L)
        .copy(onComplete = () => { released.incrementAndGet(); () }),
    )
    behavior(SendFailed("match-race".getBytes(StandardCharsets.UTF_8), "Broker unavailable"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1)
    assertEquals(responses.head.status, (KO: Status))
    assertEquals(responses.head.startTimestamp, 1_000L, "a failed request spans its start to failure detection")
    assertEquals(responses.head.endTimestamp, 4_000L)
    assertEquals(responses.head.message, Some("Broker unavailable"))
    assertEquals(released.get(), 1, "the channel reference acquisition took must be released exactly once")

    // The record is gone, so a late reply for it matches nothing and stays silent.
    behavior(replyFor("match-race"))
    assertEquals(statsEngine.responses.get().size, 1, "a reply for a failed send must not be reported")
  }

  private final class StubClock(now: Long) extends Clock {
    override def nowMillis: Long = now
  }

  private final class RecordingAction(val name: String) extends Action {
    val lastSession: AtomicReference[Session] = new AtomicReference[Session]()

    override def !(session: Session): Unit =
      execute(session)

    override def execute(session: Session): Unit =
      lastSession.set(session)
  }
}
