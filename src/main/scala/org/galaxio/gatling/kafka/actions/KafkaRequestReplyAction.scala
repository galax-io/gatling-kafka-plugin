package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.client.{KafkaMessageTracker, KafkaMessageTrackerPool}
import org.galaxio.gatling.kafka.protocol.KafkaComponents
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

  // A precondition, checked once here rather than branched on per request.
  //
  // Gatling runs this while it materialises the scenario, from `KafkaRequestReplyActionBuilder.build`, so a
  // scenario whose protocol has no consumer settings refuses to start instead of issuing one KO per virtual
  // user. The old branch answered the same question for every request, with a near-zero interval each time,
  // and the answer could never change: it depends on protocol configuration and on no session data
  // (issue #227).
  //
  // Scoped to this action deliberately. Absent consumer configuration is the produce-only shape
  // `KafkaProtocolBuilder.properties(...)` documents; publishing goes through `KafkaRequestAction`, which
  // never reads the tracker pool, and must stay unaffected.
  //
  // `getOrElse(throw …)` rather than `require`: `Predef.require` prefixes its message with
  // "requirement failed: ", which would reach the operator ahead of wording chosen to be read, and it
  // leaves the `.get` beside it guarded only by statement order. One expression is both the guard and the
  // use, and the exception type is the same one the producer-side precondition throws.
  private val trackers: KafkaMessageTrackerPool =
    components.trackersPool.getOrElse(
      throw new IllegalArgumentException(KafkaRequestFailureMessages.consumerSettingsRequired),
    )

  override def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit = {
    val requestStartDate = clock.nowMillis

    // Reports an outcome in which no record reached the broker, under a name of its own.
    //
    // Not `requestNameString`: every request entry updates the response-time digest for the name it carries,
    // KO included, and Gatling's assertion API has no successful-only scope for `responseTime`. Left on the
    // declared name, these would put samples the system under test never produced into the percentile a
    // simulation asserts on (issue #227). The name is the only field that can segregate them — for the same
    // reason the failure kind travels in the message, the response-code slot reaches no report (issue #254).
    //
    // The interval is the real one either way. A rejection that waited reports its wait; a rejection decided
    // instantly reports as much. Flattening the former to zero for uniformity would replace one untrue
    // number with another.
    def reportRejection(kind: KafkaRequestFailureMessages.RejectionKind, message: String): Unit = {
      val requestEndDate = clock.nowMillis
      statsEngine.logResponse(
        session.scenario,
        session.groups,
        KafkaRequestFailureMessages.rejectedRequestName(requestNameString, kind),
        requestStartDate,
        requestEndDate,
        KO,
        // Gatling's response-code slot, and it is discarded before any OSS report is written, so the
        // kind of failure this was travels in the message instead (issue #254).
        None,
        Some(message),
      )
      // `markAsFailed` only — deliberately not `logGroupRequestTimings`.
      //
      // That call folds the interval into the enclosing group's cumulated response time, which is a second
      // name-keyed bucket the plugin controls here. Leaving it in place moved the sample off the request's
      // own percentile and straight into `details("<group>")`, reproducing issue #227 one level up for
      // anyone who wraps a request-reply in a `group`. A rejection measured nothing, so it contributes
      // nothing to either bucket; it stays counted as a failed request under its own name.
      next ! session.markAsFailed
    }

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
        .missingCorrelationId(KafkaRequestFailureMessages.matcherName(matcher), KafkaRequestFailureMessages.remedyFor(matcher))
      logger.error(message)
      reportRejection(KafkaRequestFailureMessages.RejectionKind.NoCorrelationId, message)
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
          val token                        = registrationToken.incrementAndGet()
          // The handoff, and what the request is measured from.
          //
          // Not the request's start: the reply budget must not be charged for channel acquisition, or a
          // request can be reported as timed out moments after it was sent. And not the broker's
          // acknowledgement, which is where this used to start — that excluded the produce leg from
          // every reported time, understating what the virtual user waits for and leaving this the only
          // Gatling protocol that measures from something other than handing the request over.
          val handedOffAt                  = clock.nowMillis
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
          // Both ways a send can fail report the same way, and the log says exactly what the report
          // says: composing them separately let the two drift, and `e.getMessage` alone can be null.
          def failSend(e: Throwable): Unit = {
            val cause = KafkaRequestFailureMessages.failureCause(e)
            logger.error(cause, e)
            tracker ! KafkaMessageTracker.SendFailed(id, cause, token)
          }

          try
            components.sender.send(protocolMessage)(
              rm =>
                if (logger.underlying.isDebugEnabled) {
                  logMessage(
                    s"Record sent user=${session.userId} key=${describeBytes(protocolMessage.key)} topic=${rm.topic()}",
                    protocolMessage,
                  )
                },
              failSend,
            )
          catch {
            // The producer reports only ApiException through the callback and rethrows the rest here —
            // a closed producer, an interrupt, a serializer failure. Letting that escape would reach
            // the acquisition failure handler below, which knows nothing about the record just
            // registered.
            case NonFatal(e) => failSend(e)
          }
        },
        e => {
          val cause = KafkaRequestFailureMessages.failureCause(e)
          logger.error(cause, e)
          // Nothing was published. Approved deliberately rather than as a side effect: there is no
          // ordering that both registers before the send and still publishes when acquisition fails,
          // and publishing a request whose reply can never be received is the state issue #143 exists
          // to prevent from the other direction. The virtual user sees the same KO as before.
          reportRejection(KafkaRequestFailureMessages.RejectionKind.NoReplyChannel, cause)
        },
      )
    }
  }
}
