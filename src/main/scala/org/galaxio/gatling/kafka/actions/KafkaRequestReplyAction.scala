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
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag
import scala.util.control.NonFatal

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

  /** Makes each registration distinguishable from others that share its match id. */
  private val registrationToken = new AtomicLong(0L)

  /** What kind of failure this was, for the response-code slot a report groups and filters by.
    *
    * The exception's own type: `TimeoutException`, `RecordTooLargeException`, `NotLeaderOrFollowerException`,
    * `IllegalStateException` for a closed producer. That is the state Kafka actually reports, and it is what distinguishes "the
    * broker rejected this record" from "the client was misconfigured" when reading a run afterwards. This slot previously
    * carried `"500"` on the delivery-failure path — an HTTP status, meaningless here and identical for every cause.
    */
  private def failureType(error: Throwable): Option[String] =
    Option(error).map(e => if (e.getClass.getSimpleName.nonEmpty) e.getClass.getSimpleName else e.getClass.getName)

  /** How the configured matcher is named in a failure message.
    *
    * Two hazards, both of which [[failureType]] above already handles for exceptions: Scala objects report a trailing `$` from
    * `getSimpleName`, which would print `KafkaKeyMatcher$`, and an anonymous `new KafkaMatcher { … }` — reachable, since the
    * trait and `KafkaProtocol` are both public — reports the empty string, which would leave the message with empty parentheses
    * where the diagnostic name belongs.
    */
  private def matcherName(matcher: KafkaMatcher): String = {
    val simple = matcher.getClass.getSimpleName.stripSuffix("$")
    if (simple.nonEmpty) simple else matcher.getClass.getName
  }

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

        if (id == null) {
          // Nothing to correlate a reply on, so no correct outcome is available later — only a wrong
          // one. Under the default `matchByKey` this is a request with no key, and it used to be tracked
          // under an empty correlation id that every other keyless request also produced: they shared a
          // single slot, and a reply resolved whichever request happened to occupy it, crediting one
          // virtual user with another's answer while the real owner timed out (issue #167).
          //
          // Failed here, before acquisition and before the send, for the same reason the acquisition
          // failure below does not publish: a request whose reply could never be matched must not reach
          // the system under test. Nothing is registered at this point, so this frame owns the outcome —
          // the "every exit goes through the tracker" rule starts at registration, further down.
          val message = KafkaRequestFailureMessages
            .missingCorrelationId(matcherName(matcher), KafkaRequestFailureMessages.remedyFor(matcher))
          logger.error(message)
          reportFailure(message, None)
        } else {
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
              // Distinguishes this registration from any other sharing the same match id — the message
              // key under the default matcher, so a feeder cycling a fixed key set reuses it constantly.
              val token       = registrationToken.incrementAndGet()
              // The handoff, and what the request is measured from.
              //
              // Not the request's start: the reply budget must not be charged for channel acquisition, or a
              // request can be reported as timed out moments after it was sent. And not the broker's
              // acknowledgement, which is where this used to start — that excluded the produce leg from
              // every reported time, understating what the virtual user waits for and leaving this the only
              // Gatling protocol that measures from something other than handing the request over.
              val handedOffAt = clock.nowMillis
              tracker ! KafkaMessageTracker
                .MessagePublished(
                  id,
                  handedOffAt,
                  components.kafkaProtocol.timeout.toMillis,
                  attributes.checks,
                  session,
                  next,
                  requestNameString,
                  onComplete = () => trackers.releaseTracker(consumerTopic, matcher),
                  token = token,
                )
              // The record is registered, so from here every exit has to go through the tracker: it owns
              // the single terminal outcome and the channel reference. Reporting from this frame instead
              // would leave the record behind, and its later timeout would report the request a second
              // time and advance the same virtual user twice.
              try
                components.sender.send(protocolMessage)(
                  rm =>
                    if (logger.underlying.isDebugEnabled) {
                      logMessage(
                        s"Record sent user=${session.userId} key=${describeBytes(protocolMessage.key)} topic=${rm.topic()}",
                        protocolMessage,
                      )
                    },
                  e => {
                    logger.error(e.getMessage, e)
                    tracker ! KafkaMessageTracker.SendFailed(id, e.getMessage, token, failureType(e))
                  },
                )
              catch {
                // The producer reports only ApiException through the callback and rethrows the rest here —
                // a closed producer, an interrupt, a serializer failure. Letting that escape would reach
                // the acquisition failure handler below, which knows nothing about the record just
                // registered.
                case NonFatal(e) =>
                  logger.error(e.getMessage, e)
                  tracker ! KafkaMessageTracker.SendFailed(id, e.getMessage, token, failureType(e))
              }
            },
            e => {
              logger.error(e.getMessage, e)
              // Nothing was published. Approved deliberately rather than as a side effect: there is no
              // ordering that both registers before the send and still publishes when acquisition fails,
              // and publishing a request whose reply can never be received is the state issue #143 exists
              // to prevent from the other direction. The virtual user sees the same KO as before.
              reportFailure(e.getMessage, failureType(e))
            },
          )
        }

      case None =>
        val msg =
          s"Request-reply requires consumer settings (consumeSettings) in the Kafka protocol configuration, " +
            s"otherwise the virtual user will hang for ${components.kafkaProtocol.timeout} with no reply"
        logger.error(msg)
        reportFailure(msg, None)
    }
  }
}
