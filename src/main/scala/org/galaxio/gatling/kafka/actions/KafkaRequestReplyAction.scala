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

    components.trackersPool match {
      case Some(trackers) =>
        val consumerTopic = protocolMessage.consumerTopic
        val matcher       = components.kafkaProtocol.messageMatcher

        // Acquire the tracker eagerly here, on the Gatling actor thread, BEFORE calling sender.send().
        // tracker() may block (up to the configured timeout) waiting for Kafka partition assignment
        // via CountDownLatch.await(). It must NOT be called from inside the sender.send() onSuccess
        // callback, which runs on the Kafka producer network thread. Blocking that thread starves
        // consumer heartbeats, causing the broker to evict the consumer group after session.timeout.ms
        // and permanently poisoning the tracker pool for all subsequent virtual users. See issue #143.
        val tracker =
          try {
            trackers.tracker(
              protocolMessage.producerTopic,
              consumerTopic,
              matcher,
              None,
              components.kafkaProtocol.timeout,
            )
          } catch {
            case e: Exception =>
              val requestEndDate = clock.nowMillis
              logger.error(e.getMessage, e)
              statsEngine.logResponse(
                session.scenario,
                session.groups,
                requestNameString,
                requestStartDate,
                requestEndDate,
                KO,
                None,
                Some(e.getMessage),
              )
              next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
              return
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
              )
          },
          e => {
            // The send failed after the tracker was already acquired — release it to
            // avoid a ref-count leak that would prevent topic unsubscription.
            trackers.releaseTracker(consumerTopic, matcher)
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

      case None =>
        val requestEndDate = clock.nowMillis
        val msg            =
          s"Request-reply requires consumer settings (consumeSettings) in the Kafka protocol configuration, " +
            s"otherwise the virtual user will hang for ${components.kafkaProtocol.timeout} with no reply"
        logger.error(msg)
        statsEngine.logResponse(
          session.scenario,
          session.groups,
          requestNameString,
          requestStartDate,
          requestEndDate,
          KO,
          None,
          Some(msg),
        )
        next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
    }
  }
}
