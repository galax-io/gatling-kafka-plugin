package io.cosmospf.gatling.kafka.client

import akka.actor.ActorRef
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.cosmospf.gatling.kafka.KafkaCheck
import io.cosmospf.gatling.kafka.client.KafkaMessageTrackerActor.MessagePublished

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
