package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{ConsumerFailure, MessagePublished}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

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
    val onComplete  = new AtomicInteger(0)
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
        onComplete = () => onComplete.incrementAndGet(),
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
    assertEquals(onComplete.get(), 1)
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
