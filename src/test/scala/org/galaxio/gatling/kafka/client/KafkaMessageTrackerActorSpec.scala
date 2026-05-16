package org.galaxio.gatling.kafka.client

import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.MessagePublished
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaReplyExtraction
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class KafkaMessageTrackerActorSpec extends AnyFunSuite {

  test("timedOutMessages selects only entries whose timeout has elapsed") {
    val sentMessages = mutable.HashMap(
      KafkaMessageTrackerActor.makeTrackingKey("late".getBytes())      ->
        mutable.Queue(
          MessagePublished(
            "late".getBytes(),
            sent = 1_000L,
            replyTimeout = 2_000L,
            Nil,
            Nil,
            null,
            null,
            "req",
            silentRequest = false,
          ),
        ),
      KafkaMessageTrackerActor.makeTrackingKey("ontime".getBytes())    ->
        mutable.Queue(
          MessagePublished(
            "ontime".getBytes(),
            sent = 4_000L,
            replyTimeout = 2_000L,
            Nil,
            Nil,
            null,
            null,
            "req",
            silentRequest = false,
          ),
        ),
      KafkaMessageTrackerActor.makeTrackingKey("notimeout".getBytes()) ->
        mutable.Queue(
          MessagePublished(
            "notimeout".getBytes(),
            sent = 1_000L,
            replyTimeout = 0L,
            Nil,
            Nil,
            null,
            null,
            "req",
            silentRequest = false,
          ),
        ),
    )

    val timedOut = KafkaMessageTrackerActor.timedOutMessages(now = 3_500L, sentMessages)

    assert(timedOut.map(_.matchId.map(_.toChar).mkString).toSet == Set("late"))
  }

  test("removeTrackedMessage returns published message before timeout and none after cleanup") {
    val published: MessagePublished =
      MessagePublished(
        "reply".getBytes(),
        sent = 1_000L,
        replyTimeout = 2_000L,
        Nil,
        Nil,
        null,
        null,
        "req",
        silentRequest = false,
      )
    val sent                        =
      mutable.HashMap(KafkaMessageTrackerActor.makeTrackingKey("reply".getBytes()) -> mutable.Queue(published))

    val beforeTimeout = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)
    val afterTimeout  = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)

    assert(beforeTimeout.contains(published))
    assert(afterTimeout.isEmpty)
  }

  test("removeTimedOutMessages clears timed out entry so late replies are ignored") {
    val published: MessagePublished =
      MessagePublished(
        "reply".getBytes(),
        sent = 1_000L,
        replyTimeout = 2_000L,
        Nil,
        Nil,
        null,
        null,
        "req",
        silentRequest = false,
      )
    val sent                        =
      mutable.HashMap(KafkaMessageTrackerActor.makeTrackingKey("reply".getBytes()) -> mutable.Queue(published))

    val removed = KafkaMessageTrackerActor.removeTimedOutMessages(List(published), sent)
    val late    = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)

    assert(removed == List(published))
    assert(late.isEmpty)
  }

  test("buffered reply can be consumed when tracking is registered after message arrival") {
    val bufferedReplies = mutable.HashMap.empty[String, mutable.Queue[KafkaMessageTrackerActor.BufferedReply]]
    val message         = KafkaProtocolMessage("reply".getBytes(), "value".getBytes(), "requests", "replies")

    KafkaMessageTrackerActor.bufferReply("reply".getBytes(), received = 5_000L, message, bufferedReplies)

    val buffered = KafkaMessageTrackerActor.removeBufferedReply("reply".getBytes(), bufferedReplies)

    assert(buffered.exists(_.message == message))
    assert(bufferedReplies.isEmpty)
  }

  test("removeTrackedMessage dequeues pending waiters sharing the same match id in FIFO order") {
    val first  = MessagePublished("same".getBytes(), 1_000L, 5_000L, Nil, Nil, null, null, "first", silentRequest = false)
    val second = MessagePublished("same".getBytes(), 2_000L, 5_000L, Nil, Nil, null, null, "second", silentRequest = false)
    val sent   =
      mutable.HashMap(KafkaMessageTrackerActor.makeTrackingKey("same".getBytes()) -> mutable.Queue(first, second))

    val firstRemoved  = KafkaMessageTrackerActor.removeTrackedMessage("same".getBytes(), sent)
    val secondRemoved = KafkaMessageTrackerActor.removeTrackedMessage("same".getBytes(), sent)

    assert(firstRemoved.contains(first))
    assert(secondRemoved.contains(second))
    assert(sent.isEmpty)
  }

  test("removeExpiredBufferedReplies evicts stale unmatched replies") {
    val bufferedReplies = mutable.HashMap(
      KafkaMessageTrackerActor.makeTrackingKey("stale".getBytes()) ->
        mutable.Queue(
          KafkaMessageTrackerActor.BufferedReply(
            "stale".getBytes(),
            received = 1_000L,
            KafkaProtocolMessage("stale".getBytes(), "value".getBytes(), "requests", "replies"),
          ),
        ),
      KafkaMessageTrackerActor.makeTrackingKey("fresh".getBytes()) ->
        mutable.Queue(
          KafkaMessageTrackerActor.BufferedReply(
            "fresh".getBytes(),
            received = 59_500L,
            KafkaProtocolMessage("fresh".getBytes(), "value".getBytes(), "requests", "replies"),
          ),
        ),
    )

    KafkaMessageTrackerActor.removeExpiredBufferedReplies(now = 61_500L, bufferedReplies)

    assert(bufferedReplies.keySet == Set(KafkaMessageTrackerActor.makeTrackingKey("fresh".getBytes())))
  }

  test("applyReplyExtractions stores extracted values into Gatling session") {
    val session = Session("scenario", 1L, null)
    val message = KafkaProtocolMessage(
      key = "request-key".getBytes(),
      value = "reply-value".getBytes(),
      inputTopic = "requests",
      outputTopic = "replies",
    )

    val result = KafkaMessageTrackerActor.applyReplyExtractions(
      session,
      message,
      List(
        KafkaReplyExtraction("replyValue", msg => new String(msg.value)),
        KafkaReplyExtraction("replyKey", msg => new String(msg.key)),
      ),
    )

    assert(result.exists(_.attributes == Map("replyValue" -> "reply-value", "replyKey" -> "request-key")))
  }

  test("applyReplyExtractions rejects null extracted values") {
    val result = KafkaMessageTrackerActor.applyReplyExtractions(
      Session("scenario", 1L, null),
      KafkaProtocolMessage("k".getBytes(), "v".getBytes(), "requests", "replies"),
      List(KafkaReplyExtraction("replyValue", _ => null)),
    )

    assert(result == Left("reply extraction 'replyValue' returned null"))
  }
}
