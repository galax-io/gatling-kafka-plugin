package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session._
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.reflect.ClassTag

final class KafkaRequestAction[K: ClassTag, V: ClassTag](
    val components: KafkaComponents,
    val attributes: KafkaAttributes[K, V],
    val coreComponents: CoreComponents,
    val next: Action,
    val throttler: Option[ActorRef[Throttler.Command]],
) extends KafkaAction[K, V](components, attributes, throttler) {

  override def name: String    = genName("kafkaRequest")
  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock: Clock             = coreComponents.clock

  private def reportUnbuildableRequest(session: Session, error: String): Unit = {
    val loggedName = attributes.requestName(session) match {
      case Success(requestNameValue) =>
        statsEngine.logRequestCrash(session.scenario, session.groups, requestNameValue, s"Failed to build request: $error")
        requestNameValue
      case _                         => name
    }
    logger.error(s"'$loggedName' failed to execute: $error")
  }

  override def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit = {
    val requestStartDate = clock.nowMillis
    components.sender.send(protocolMessage)(
      metadata => {
        val requestEndDate = clock.nowMillis
        if (logger.underlying.isDebugEnabled) {
          logger.debug(s"Record sent user=${session.userId} key=${new String(protocolMessage.key)} topic=${metadata.topic()}")
          logger.trace(s"ProducerRecord=${protocolMessage.toProducerRecord}")
        }

        statsEngine.logResponse(
          session.scenario,
          session.groups,
          requestNameString,
          startTimestamp = requestStartDate,
          endTimestamp = requestEndDate,
          OK,
          None,
          None,
        )
        next ! session.logGroupRequestTimings(requestStartDate, requestEndDate)
      },
      exception => {
        val requestEndDate = clock.nowMillis

        logger.error(exception.getMessage, exception)
        reportUnbuildableRequest(session, exception.getMessage)

        statsEngine.logResponse(
          session.scenario,
          session.groups,
          requestNameString,
          startTimestamp = requestStartDate,
          endTimestamp = requestEndDate,
          KO,
          None,
          Some(exception.getMessage),
        )
        next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
      },
    )
  }
}
