package org.galaxio.gatling.kafka.client

import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerActor.MessagePublished

class KafkaMessageTracker(actor: ActorRef[KafkaMessageTrackerActor.KafkaMessage]) {

  def track(
      matchId: Array[Byte],
      sent: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      session: Session,
      next: Action,
      requestName: String,
  ): Unit =
    actor ! MessagePublished(
      matchId,
      sent,
      replyTimeout,
      checks,
      session,
      next,
      requestName,
    )
}
