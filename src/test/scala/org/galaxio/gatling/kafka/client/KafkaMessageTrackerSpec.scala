package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.{ConsumerFailure, MessageConsumed, MessagePublished}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

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
