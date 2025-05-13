package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.reflect.ClassTag

class KafkaRequestReplyAction[K: ClassTag, V: ClassTag](
    components: KafkaComponents,
    attributes: KafkaAttributes[K, V],
    coreComponents: CoreComponents,
    val next: Action,
    throttler: Option[ActorRef[Throttler.Command]],
) extends KafkaAction[K, V](components, attributes, throttler) {

  override def name: String    = genName("kafkaRequestReply")
  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock: Clock             = coreComponents.clock

  override def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit = {
    val requestStartDate = clock.nowMillis
    components.sender.send(protocolMessage)(
      rm => {
        if (logger.underlying.isDebugEnabled) {
          logMessage(
            s"Record sent user=${session.userId} key=${new String(protocolMessage.key)} topic=${rm.topic()}",
            protocolMessage,
          )
        }
        val id = components.kafkaProtocol.messageMatcher.requestMatch(protocolMessage)

        components.trackersPool.map { trackers =>
          val tracker = trackers.tracker(
            protocolMessage.producerTopic,
            protocolMessage.consumerTopic,
            components.kafkaProtocol.messageMatcher,
            None,
            components.kafkaProtocol.timeout,
          )
          tracker ! KafkaMessageTracker
            .MessagePublished(
              id,
              clock.nowMillis,
              components.kafkaProtocol.timeout.toMillis,
              attributes.checks,
              session,
              next,
              requestNameString,
            )
        }
      },
      e => {
        val requestEndDate = clock.nowMillis
        logger.error(e.getMessage, e)
        statsEngine.logResponse(
          session.scenario,
          session.groups,
          requestNameString,
          requestStartDate,
          requestEndDate,
          KO,
          Some("500"),
          Some(e.getMessage),
        )
        next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
      },
    )
  }
}
