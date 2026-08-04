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

    components.trackersPool match {
      case Some(trackers) =>
        val consumerTopic = protocolMessage.consumerTopic
        val matcher       = components.kafkaProtocol.messageMatcher
        val id            = matcher.requestMatch(protocolMessage)

        // Acquire, register, then send — in that order, and the order is the point.
        //
        // Sending first meant the request was on the wire, and answerable, before anything was watching
        // for its answer: the pending record was only created from the producer's acknowledgement
        // callback, which under load can run after a fast responder's reply has already been polled and
        // broadcast. The reply then matched nothing, was discarded silently, and the request failed on
        // its reply timeout — indistinguishable from a system under test that never answered (issue
        // #191).
        //
        // Registering first replaces that race with a causal chain: the record is enqueued before the
        // send, a reply cannot exist before the send, and Gatling's mailbox preserves enqueue order
        // across producer threads. Acquisition is asynchronous (issue #163), so this still does not
        // block the virtual user.
        trackers.acquireTracker(
          protocolMessage.producerTopic,
          consumerTopic,
          matcher,
          None,
          components.kafkaProtocol.timeout,
        )(
          tracker => {
            tracker ! KafkaMessageTracker
              .MessagePublished(
                id,
                requestStartDate,
                components.kafkaProtocol.timeout.toMillis,
                attributes.checks,
                session,
                next,
                requestNameString,
                onComplete = () => trackers.releaseTracker(consumerTopic, matcher),
              )
            components.sender.send(protocolMessage)(
              rm => {
                if (logger.underlying.isDebugEnabled) {
                  logMessage(
                    s"Record sent user=${session.userId} key=${describeBytes(protocolMessage.key)} topic=${rm.topic()}",
                    protocolMessage,
                  )
                }
                // Carries the timestamp a successful request-reply is measured from. Reporting stays on
                // the acknowledgement, exactly as before, even though registration now happens earlier.
                tracker ! KafkaMessageTracker.MessageAcked(id, clock.nowMillis)
              },
              e => {
                logger.error(e.getMessage, e)
                // Through the tracker rather than reported here: the record registered above has to be
                // removed and the channel reference released, and the tracker owns both.
                tracker ! KafkaMessageTracker.SendFailed(id, e.getMessage)
              },
            )
          },
          e => {
            logger.error(e.getMessage, e)
            // Nothing was published. Approved deliberately rather than as a side effect: there is no
            // ordering that both registers before the send and still publishes when acquisition fails,
            // and publishing a request whose reply can never be received is the state issue #143 exists
            // to prevent from the other direction. The virtual user sees the same KO as before.
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
  }
}
