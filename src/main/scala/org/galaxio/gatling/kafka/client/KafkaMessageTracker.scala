package org.galaxio.gatling.kafka.client

import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.{MessagePublished, TrackerMessage}
import org.galaxio.gatling.kafka.request.builder.KafkaReplyExtraction

class KafkaMessageTracker(actor: ActorRef[TrackerMessage]) {

  def track(
      matchId: Array[Byte],
      sent: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      replyExtractions: List[KafkaReplyExtraction],
      session: Session,
      next: Action,
      requestName: String,
      silentRequest: Boolean,
  ): Unit =
    actor ! MessagePublished(
      matchId,
      sent,
      replyTimeout,
      checks,
      replyExtractions,
      session,
      next,
      requestName,
      silentRequest,
    )
}
