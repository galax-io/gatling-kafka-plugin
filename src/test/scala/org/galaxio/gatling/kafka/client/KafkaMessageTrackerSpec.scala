package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, ActorSystem}
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{
  ConsumerFailure,
  MessageAcked,
  MessageConsumed,
  MessagePublished,
  SendFailed,
}
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

  /** Whether the tracker is still holding an armed timeout scan.
    *
    * Read off the actor instance rather than off Gatling's scheduler: the scheduler is an
    * `Executors.newSingleThreadScheduledExecutor`, whose ScheduledThreadPoolExecutor sits behind a JDK-internal delegate that
    * JDK 17 will not open to reflection, so its queue cannot be counted from a test.
    *
    * Behaviour alone cannot distinguish the two halves of the fix. `die` swaps the behaviour to one that drops messages, so a
    * scan left running still produces nothing observable — it just keeps firing at a dead actor once a second, on a scheduler
    * thread shared by the whole simulation, holding the tracker reachable. That is the leak, and this is what shows it.
    */
  private def scanIsArmed(tracker: Actor[KafkaMessageTracker.TrackerMessage]): Boolean = {
    val field = tracker.getClass.getDeclaredField("periodicTimeoutScan")
    field.setAccessible(true)
    field.get(tracker).asInstanceOf[Option[_]].isDefined
  }

  private def publishedWithTimeout(
      next: Action,
      session: Session,
      replyTimeout: Long,
  ): MessagePublished =
    MessagePublished(
      matchId = "match-stop".getBytes(StandardCharsets.UTF_8),
      sentTimestamp = 0L,
      replyTimeout = replyTimeout,
      checks = Nil,
      session = session,
      next = next,
      requestName = "request-reply",
    )

  test("Stop cancels the periodic timeout scan and stops the tracker") {
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      // Far enough ahead that the pending request below is already past its 50 ms reply timeout, so any
      // scan that still ran would have something to report.
      val clock       = new StubClock(10_000L)
      val next        = new RecordingAction("next")
      val tracker     =
        KafkaMessageTracker.actor[Array[Byte], Array[Byte]]("tracker", statsEngine, clock, KafkaKeyMatcher, None)
      val ref         = actorSystem.actorOf(tracker)

      ref ! publishedWithTimeout(next, Session("scenario", 1L, null), replyTimeout = 50L)
      Thread.sleep(300)
      assert(scanIsArmed(tracker), "a request with a reply timeout must arm the periodic scan")

      ref ! KafkaMessageTracker.Stop
      Thread.sleep(300)

      assert(
        !scanIsArmed(tracker),
        "Stop must cancel the periodic scan: leaving it armed keeps firing at a dead actor once a second " +
          "for the rest of the run and holds the tracker reachable, which is the leak in issue #166",
      )
      assertEquals(statsEngine.responses.get().size, 0, "a stopped tracker must report nothing")
      assert(next.lastSession.get() == null, "a stopped tracker must not advance any virtual user")
    }
  }

  test("Stop is safe on a tracker that never armed a timeout scan") {
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      val next        = new RecordingAction("next")
      val tracker     = KafkaMessageTracker
        .actor[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(1_000L), KafkaKeyMatcher, None)
      val ref         = actorSystem.actorOf(tracker)

      // replyTimeout 0 never arms a scan, so Stop has nothing to cancel.
      ref ! publishedWithTimeout(next, Session("scenario", 1L, null), replyTimeout = 0L)
      ref ! KafkaMessageTracker.Stop
      Thread.sleep(500)

      assert(!scanIsArmed(tracker), "an unarmed tracker must still be unarmed after Stop")
      assertEquals(statsEngine.responses.get().size, 0, "stopping an unarmed tracker must not report anything")
    }
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

  test("a reply arriving before its own acknowledgement is reported at once, not held") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    // The order this feature exists for: registered, answered, and only then acknowledged. Before the
    // fix the request was not registered until the acknowledgement, so the reply found nothing and was
    // dropped — the request then failed on its reply timeout with no sign a reply had ever arrived.
    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L))
    behavior(replyFor("match-race"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1, "a reply that arrived must be reported, never withheld pending another message")
    assertEquals(responses.head.status, (OK: Status))
    assertEquals(
      responses.head.startTimestamp,
      1_000L,
      "with no acknowledgement yet, the request's own start is used rather than losing the reply",
    )
    assertEquals(responses.head.endTimestamp, 5_000L)

    // The acknowledgement arrives for a request that is already complete. It must not resurrect the
    // record, or nothing would ever resolve it and it would report a second, spurious timeout.
    behavior(MessageAcked("match-race".getBytes(StandardCharsets.UTF_8), sentTimestamp = 2_000L))
    assertEquals(statsEngine.responses.get().size, 1, "a late acknowledgement must not re-register a completed request")
  }

  test("a reply arriving after its acknowledgement is reported immediately, from the acknowledgement") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L))
    behavior(MessageAcked("match-race".getBytes(StandardCharsets.UTF_8), sentTimestamp = 2_000L))
    behavior(replyFor("match-race"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1)
    assertEquals(responses.head.status, (OK: Status))
    assertEquals(responses.head.startTimestamp, 2_000L)
    assertEquals(responses.head.endTimestamp, 5_000L)
  }

  test("a request whose acknowledgement never arrives still times out, measured from its start") {
    // Needs an ActorSystem: a positive reply timeout arms the periodic scan, which touches `scheduler`.
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      val next        = new RecordingAction("next")
      val ref         = actorSystem.actorOf(
        KafkaMessageTracker
          .actor[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None),
      )

      // Registered at 1_000 with a 5 s timeout and never acknowledged; the clock is at 9_000. Measuring
      // from the acknowledgement would leave this pending forever, because there is none.
      ref ! published(next, replyTimeout = 5_000L, requestStart = 1_000L)
      ref ! KafkaMessageTracker.TimeoutScan
      Thread.sleep(500)

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "a request stuck without an acknowledgement must still time out")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(responses.head.startTimestamp, 1_000L)
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
