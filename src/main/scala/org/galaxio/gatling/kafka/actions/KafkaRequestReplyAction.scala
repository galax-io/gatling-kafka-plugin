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
) extends KafkaAction[K, V](attributes, throttler) {

  override def name: String    = genName("kafkaRequestReply")
  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock: Clock             = coreComponents.clock

  override def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit = {
    val requestStartDate = clock.nowMillis

    def reportFailure(message: String, responseCode: Option[String]): Unit = {
      val requestEndDate = clock.nowMillis
      statsEngine.logResponse(
        session.scenario,
        session.groups,
        requestNameString,
        requestStartDate,
        requestEndDate,
        KO,
        responseCode,
        Some(message),
      )
      next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
    }

    components.sender.send(protocolMessage)(
      rm => {
        if (logger.underlying.isDebugEnabled) {
          logMessage(
            s"Record sent user=${session.userId} key=${describeBytes(protocolMessage.key)} topic=${rm.topic()}",
            protocolMessage,
          )
        }
        val id = components.kafkaProtocol.messageMatcher.requestMatch(protocolMessage)

        components.trackersPool match {
          case Some(trackers) =>
            val consumerTopic = protocolMessage.consumerTopic
            val matcher       = components.kafkaProtocol.messageMatcher
            // This body runs on the producer's I/O thread: acquisition hands back a tracker without
            // waiting here, so a reply topic that is slow to be assigned cannot stall that thread.
            trackers.acquireTracker(
              protocolMessage.producerTopic,
              consumerTopic,
              matcher,
              None,
              components.kafkaProtocol.timeout,
            )(
              tracker =>
                tracker ! KafkaMessageTracker
                  .MessagePublished(
                    id,
                    clock.nowMillis,
                    components.kafkaProtocol.timeout.toMillis,
                    attributes.checks,
                    session,
                    next,
                    requestNameString,
                    onComplete = () => trackers.releaseTracker(consumerTopic, matcher),
                  ),
              e => {
                logger.error(e.getMessage, e)
                reportFailure(e.getMessage, None)
              },
            )
          case None           =>
            val msg =
              s"Request-reply requires consumer settings (consumeSettings) in the Kafka protocol configuration, " +
                s"otherwise the virtual user will hang for ${components.kafkaProtocol.timeout} with no reply"
            logger.error(msg)
            reportFailure(msg, None)
        }
      },
      e => {
        logger.error(e.getMessage, e)
        reportFailure(e.getMessage, Some("500"))
      },
    )
  }
}
