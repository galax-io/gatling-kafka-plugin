package org.galaxio.gatling.kafka.client

import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.MessagePublished
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class KafkaMessageTrackerActorSpec extends AnyFunSuite {

  test("timedOutMessages selects only entries whose timeout has elapsed") {
    val sentMessages = mutable.HashMap(
      KafkaMessageTrackerActor.makeTrackingKey("late".getBytes())      ->
        MessagePublished(
          "late".getBytes(),
          sent = 1_000L,
          replyTimeout = 2_000L,
          Nil,
          null,
          null,
          "req",
          silentRequest = false,
        ),
      KafkaMessageTrackerActor.makeTrackingKey("ontime".getBytes())    ->
        MessagePublished(
          "ontime".getBytes(),
          sent = 4_000L,
          replyTimeout = 2_000L,
          Nil,
          null,
          null,
          "req",
          silentRequest = false,
        ),
      KafkaMessageTrackerActor.makeTrackingKey("notimeout".getBytes()) ->
        MessagePublished(
          "notimeout".getBytes(),
          sent = 1_000L,
          replyTimeout = 0L,
          Nil,
          null,
          null,
          "req",
          silentRequest = false,
        ),
    )

    val timedOut = KafkaMessageTrackerActor.timedOutMessages(now = 3_500L, sentMessages)

    assert(timedOut.map(_.matchId.map(_.toChar).mkString).toSet == Set("late"))
  }

  test("removeTrackedMessage returns published message before timeout and none after cleanup") {
    val published =
      MessagePublished("reply".getBytes(), sent = 1_000L, replyTimeout = 2_000L, Nil, null, null, "req", silentRequest = false)
    val sent      = mutable.HashMap(KafkaMessageTrackerActor.makeTrackingKey("reply".getBytes()) -> published)

    val beforeTimeout = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)
    val afterTimeout  = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)

    assert(beforeTimeout.contains(published))
    assert(afterTimeout.isEmpty)
  }

  test("removeTimedOutMessages clears timed out entry so late replies are ignored") {
    val published =
      MessagePublished("reply".getBytes(), sent = 1_000L, replyTimeout = 2_000L, Nil, null, null, "req", silentRequest = false)
    val sent      = mutable.HashMap(KafkaMessageTrackerActor.makeTrackingKey("reply".getBytes()) -> published)

    val removed = KafkaMessageTrackerActor.removeTimedOutMessages(List(published), sent)
    val late    = KafkaMessageTrackerActor.removeTrackedMessage("reply".getBytes(), sent)

    assert(removed == List(published))
    assert(late.isEmpty)
  }
}
