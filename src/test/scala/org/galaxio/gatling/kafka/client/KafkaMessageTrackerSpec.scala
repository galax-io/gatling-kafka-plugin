package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Validation
import io.gatling.core.check.{Check, CheckResult}
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, ActorSystem}
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.RecordingStatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{ConsumerFailure, MessageConsumed, MessagePublished, SendFailed}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.KafkaCheck

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

      ref ! KafkaMessageTracker.Stop("released by test")
      Thread.sleep(300)

      assert(
        !scanIsArmed(tracker),
        "Stop must cancel the periodic scan: leaving it armed keeps firing at a dead actor once a second " +
          "for the rest of the run and holds the tracker reachable, which is the leak in issue #166",
      )
      // The scan that would otherwise have timed this request out has just been cancelled, so Stop has
      // to resolve it here. Dropping it would leave the virtual user with no outcome at all.
      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "a request pending when the tracker is stopped must still be reported")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(responses.head.message, Some("released by test"))
      assert(next.lastSession.get() != null, "the virtual user behind it must still be advanced")
    }
  }

  test("Stop is safe on a tracker that never armed a timeout scan") {
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      val next        = new RecordingAction("next")
      val tracker     = KafkaMessageTracker
        .actor[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(1_000L), KafkaKeyMatcher, None)
      val ref         = actorSystem.actorOf(tracker)

      // replyTimeout 0 never arms a scan, so Stop has nothing to cancel — but the pending request still
      // has to be resolved, and with no scan there is nothing else that ever could.
      ref ! publishedWithTimeout(next, Session("scenario", 1L, null), replyTimeout = 0L)
      ref ! KafkaMessageTracker.Stop("released by test")
      Thread.sleep(500)

      assert(!scanIsArmed(tracker), "an unarmed tracker must still be unarmed after Stop")
      assertEquals(statsEngine.responses.get().size, 1, "a request with no timeout must still be resolved by Stop")
    }
  }

  test("a registration that races Stop is failed, not dropped") {
    withActorSystem { actorSystem =>
      val statsEngine = new RecordingStatsEngine
      val next        = new RecordingAction("next")
      val ref         = actorSystem.actorOf(
        KafkaMessageTracker
          .actor[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(1_000L), KafkaKeyMatcher, None),
      )

      // The pool checks for consumer failure before handing a tracker over and the consumer can fail in
      // the gap, so a registration can legitimately arrive after Stop. Gatling's `die` would drop it and
      // the virtual user would hang for the rest of the run with no success and no failure.
      ref ! KafkaMessageTracker.Stop("Consumer failure: boom")
      ref ! publishedWithTimeout(next, Session("scenario", 1L, null), replyTimeout = 0L)
      Thread.sleep(500)

      val responses = statsEngine.responses.get()
      assertEquals(responses.size, 1, "a registration arriving after Stop must still get an outcome")
      assertEquals(responses.head.status, (KO: Status))
      assertEquals(responses.head.message, Some("Consumer failure: boom"))
      assert(next.lastSession.get() != null, "the virtual user must be advanced rather than left hanging")
    }
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

  // Issue #167. `matchKeyFor` used to fold a null match id into `Array.emptyByteArray`, so "this request
  // has no identity" and "this request's identity is empty" became the same map key. Every keyless
  // request-reply therefore shared one slot, and a reply carrying an empty key resolved whichever request
  // happened to be occupying it.
  test("an absent match id and an empty one are different requests") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L).copy(matchId = null))
    // A real reply from a keyless round trip: Kafka distinguishes an absent key from an empty one, and so
    // must the correlation table. This reply belongs to some other request, not to the one above.
    behavior(
      MessageConsumed(
        received = 5_000L,
        message = KafkaProtocolMessage(
          key = Array.emptyByteArray,
          value = "reply".getBytes(StandardCharsets.UTF_8),
          producerTopic = "reply-topic",
          consumerTopic = "reply-topic",
        ),
      ),
    )

    // Exactly one outcome, and it is the refusal — not a match. Before the fix `matchKeyFor` folded null
    // onto the empty array, so this reply resolved the registration and reported it a second time.
    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1, "a reply whose match id is empty must not resolve a request registered with none")
    assert(
      responses.head.message.exists(_.contains("no match id")),
      s"the single outcome must be the refusal, not a match: ${responses.head.message}",
    )
  }

  // Issue #168, second layer. Fixing the preparers stops the tombstone NPE, but the guarantee worth
  // having is "no check can strand a virtual user" — which has to hold for check types this plugin does
  // not own. Before the catch, an exception out of Check.check left the record already removed from
  // sentMessages (so no timeout scan could fail it) and `next ! session` unreached: the user stopped
  // with nothing in the report.
  test("a check that throws is reported as a failure instead of stranding the virtual user") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val released    = new AtomicInteger(0)
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    val exploding: KafkaCheck = new KafkaCheck {
      override def check(
          response: KafkaProtocolMessage,
          session: Session,
          preparedCache: java.util.Map[Any, Any],
      ): Validation[CheckResult] = throw new RuntimeException("check blew up")

      override def checkIf(condition: Expression[Boolean]): Check[KafkaProtocolMessage]                                    = this
      override def checkIf(condition: (KafkaProtocolMessage, Session) => Validation[Boolean]): Check[KafkaProtocolMessage] =
        this
    }

    behavior(
      published(next, replyTimeout = 0L, requestStart = 1_000L)
        .copy(checks = List(exploding), onComplete = () => { released.incrementAndGet(); () }),
    )
    behavior(replyFor("match-race"))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 1, "a throwing check must still produce an outcome")
    assertEquals(responses.head.status, (KO: Status))
    assert(
      responses.head.message.exists(_.contains("check blew up")),
      s"the failure must carry the cause: ${responses.head.message}",
    )
    assert(next.lastSession.get() != null, "and the virtual user must be advanced, not left hanging")
    assertEquals(released.get(), 1, "the channel reference must still be released exactly once")
  }

  test("a registration with no match id is refused rather than stored") {
    val statsEngine = new RecordingStatsEngine
    val next        = new RecordingAction("next")
    val tracker     =
      new KafkaMessageTracker[Array[Byte], Array[Byte]]("tracker", statsEngine, new StubClock(9_000L), KafkaKeyMatcher, None)
    val behavior    = tracker.init()

    // The sending side rejects these before they get here, so this is the symmetric guard to the one
    // MessageConsumed already has. It matters because the table cannot hold a null safely:
    // `Arrays.equals(null, null)` is true, so two of them would alias each other and re-create issue #167
    // one key over — with a displacement message telling a user who supplied no key to make their key
    // distinct.
    behavior(published(next, replyTimeout = 0L, requestStart = 1_000L).copy(matchId = null))
    behavior(published(next, replyTimeout = 0L, requestStart = 2_000L).copy(matchId = null, token = 2L))

    val responses = statsEngine.responses.get()
    assertEquals(responses.size, 2, "each refused registration must still be reported, not dropped")
    assert(
      responses.forall(_.message.exists(_.contains("no match id"))),
      s"unexpected messages: ${responses.flatMap(_.message)}",
    )
    assert(
      responses.forall(!_.message.exists(_.contains("reused"))),
      "a request that supplied no id must not be told it reused one",
    )
    assert(next.lastSession.get() != null, "the virtual user must be advanced rather than left hanging")
  }

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
