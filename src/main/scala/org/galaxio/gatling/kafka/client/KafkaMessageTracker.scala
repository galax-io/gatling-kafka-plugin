package org.galaxio.gatling.kafka.client

import akka.actor.ActorRef
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.KafkaCheck
import KafkaMessageTrackerActor.MessagePublished

class KafkaMessageTracker(actor: ActorRef) {

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
