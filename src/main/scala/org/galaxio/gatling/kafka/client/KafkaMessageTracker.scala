package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, Behavior, Cancellable}
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.{KafkaCheck, KafkaLogging}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object KafkaMessageTracker {

  def actor[K, V](
      actorName: String,
      statsEngine: StatsEngine,
      clock: Clock,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): Actor[TrackerMessage] =
    new KafkaMessageTracker[K, V](actorName, statsEngine, clock, messageMatcher, responseTransformer)

  sealed trait TrackerMessage

  final case class MessagePublished(
      matchId: Array[Byte],
      sentTimestamp: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      session: Session,
      next: Action,
      requestName: String,
      onComplete: () => Unit = () => (),
  ) extends TrackerMessage

  final case class MessageConsumed(
      received: Long,
      message: KafkaProtocolMessage,
  ) extends TrackerMessage

  /** The broker acknowledged the request identified by `matchId`, at `sentTimestamp`.
    *
    * Separate from [[MessagePublished]] because the request is now registered *before* it is handed to the producer (issue
    * #191), so the acknowledgement timestamp does not exist yet at registration. It is the timestamp a successful request-reply
    * is measured from, and keeping it that way is what leaves reported response times unchanged.
    */
  final case class MessageAcked(matchId: Array[Byte], sentTimestamp: Long) extends TrackerMessage

  /** The producer failed to deliver the request identified by `matchId`.
    *
    * The request was registered before the send, so a delivery failure has to un-register it — otherwise the record sits until
    * its reply timeout and, worse, its channel never returns to idle because nothing releases the reference acquisition took.
    */
  final case class SendFailed(matchId: Array[Byte], errorMessage: String) extends TrackerMessage

  final case class ConsumerFailure(errorMessage: String) extends TrackerMessage

  /** Asks the tracker to cancel its periodic timeout scan and stop.
    *
    * Sent by [[KafkaMessageTrackerPool]] once the reply channel this tracker belongs to has been released. It exists because
    * Gatling's `ActorSystem` offers no way to stop an actor from outside — `die` is an `Effect` reachable only from the actor's
    * own behaviour — so stopping has to be asked for (issue #166).
    */
  private[client] final case object Stop extends TrackerMessage

  /** Package-visible so a spec can drive one scan deterministically, rather than waiting on the scheduler. */
  private[client] final case object TimeoutScan extends TrackerMessage

  /** One request that has been registered and is awaiting its reply.
    *
    * `published.sentTimestamp` is the moment the virtual user began the request, captured before the channel was even acquired.
    * `ackedAt` is when the broker acknowledged it, and arrives separately because registration now happens before the send
    * (issue #191).
    *
    * In the companion rather than in the class body: a case class nested in a class carries an outer reference that pattern
    * matching cannot check at run time, which this build treats as an error.
    */
  private final case class PendingRequest(published: MessagePublished, ackedAt: Option[Long] = None) {

    /** The timestamp this request is reported from, and the one its reply timeout is measured against.
      *
      * The acknowledgement once it has landed, which is what a successful request-reply has always been measured from and what
      * keeps reported response times unchanged. Otherwise the value the caller registered with, which is the request's own
      * start.
      *
      * A reply is deliberately never held back waiting for the acknowledgement it is nominally measured from. Holding would
      * make a reply that already arrived depend on a later message to be reported at all — the same shape as the defect this
      * fixes, and it would lose the reply outright if that message never came. Reporting a hair early is the cheaper error: the
      * gap is a single produce acknowledgement, and it only opens for a reply that outran one.
      */
    def startedAt: Long = ackedAt.getOrElse(published.sentTimestamp)
  }

  private def makeKeyForSentMessages(m: Array[Byte]): String =
    Option(m).map(java.util.Base64.getEncoder.encodeToString(_)).getOrElse("")
}

/** Actor to record request and response Kafka Events, publishing data to the Gatling core DataWriter
  */
class KafkaMessageTracker[K, V](
    name: String,
    statsEngine: StatsEngine,
    clock: Clock,
    messageMatcher: KafkaMatcher,
    responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
) extends Actor[TrackerMessage](name) with KafkaLogging {

  private val sentMessages     = mutable.HashMap.empty[String, PendingRequest]
  private val timedOutMessages = mutable.ArrayBuffer.empty[PendingRequest]

  /** The periodic timeout scan, once something with a reply timeout has been published.
    *
    * Retained rather than discarded so [[KafkaMessageTracker.Stop]] can cancel it. The handle held nothing before, which meant
    * the scan outlived every channel that ever armed one: it captures `self`, so the tracker — and with it the stats engine,
    * the clock and the matcher closures — stayed reachable from the actor system's scheduler, and that scheduler is a single
    * thread shared by the whole simulation, so each leaked scan also cost one wakeup per second for the rest of the run (issue
    * #166).
    */
  private var periodicTimeoutScan: Option[Cancellable] = None

  private def triggerPeriodicTimeoutScan(): Unit =
    if (periodicTimeoutScan.isEmpty) {
      periodicTimeoutScan = Some(scheduler.scheduleAtFixedRate(1000.millis) {
        self ! TimeoutScan
      })
    }

  /** Reports a matched reply and drops its record, whether or not the acknowledgement has landed yet. */
  private def completeMatched(
      key: String,
      pending: PendingRequest,
      receivedTimestamp: Long,
      message: KafkaProtocolMessage,
  ): Unit = {
    sentMessages.remove(key)
    val published = pending.published
    try
      processMessage(
        published.session,
        pending.startedAt,
        receivedTimestamp,
        published.checks,
        message,
        published.next,
        published.requestName,
      )
    finally published.onComplete()
  }

  override def init(): Behavior[TrackerMessage] = {
    // The request is registered here, before it is handed to the producer, so a reply cannot be looked
    // up before the record for it exists (issue #191). The acknowledgement timestamp is not known yet
    // and arrives as MessageAcked.
    case messageSent: MessagePublished =>
      val key = makeKeyForSentMessages(messageSent.matchId)
      logger.debug("Published with MatchId: {} Tracking Key: {}", describeBytes(messageSent.matchId), key)
      sentMessages += key -> PendingRequest(messageSent)
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
      }
      stay

    case MessageAcked(matchId, sentTimestamp) =>
      val key = makeKeyForSentMessages(matchId)
      // Only if the request is still pending. A reply that outran its own acknowledgement has already
      // completed and removed the record, and re-adding it here would leave a record nothing ever
      // resolves — it would sit until its reply timeout and report a KO for a request that succeeded.
      sentMessages.get(key).foreach(pending => sentMessages += key -> pending.copy(ackedAt = Some(sentTimestamp)))
      stay

    case SendFailed(matchId, errorMessage) =>
      val key = makeKeyForSentMessages(matchId)
      sentMessages.remove(key).foreach { pending =>
        val published = pending.published
        logger.error("Delivery failed for {}: {}", describeBytes(matchId), errorMessage)
        try
          executeNext(
            published.session.markAsFailed,
            pending.startedAt,
            clock.nowMillis,
            KO,
            published.next,
            published.requestName,
            Some("500"),
            Some(errorMessage),
          )
        // Releases the reference acquisition took. Without it the channel never returns to idle and is
        // never reclaimed, because the request that held it was never completed by any other path.
        finally published.onComplete()
      }
      stay

    // message was received; publish stats and remove from the map
    case MessageConsumed(receivedTimestamp, forTransformMessage) =>
      val message = responseTransformer.map(_(forTransformMessage)).getOrElse(forTransformMessage)
      val replyId = messageMatcher.responseMatch(message)
      if (replyId == null) {
        logger.error("no messageMatcher key for read message {}", message.key)
      } else {
        if (message.key == null || message.value == null) {
          logger.warn(" --- received message with null key or value")
        } else {
          logger.trace(" --- received key={} value={}", describeBytes(message.key), describeBytes(message.value))
        }

        val messageKey = describeBytes(message.key)
        logMessage(s"Record received key=$messageKey", message)
        val key        = makeKeyForSentMessages(replyId)
        logger.debug(
          "Received with MatchId: {} Tracking Key: {}, producerTopic: {}, consumerTopic: {}",
          describeBytes(replyId),
          key,
          message.producerTopic,
          message.consumerTopic,
        )
        // Reported as soon as it arrives, whether or not the acknowledgement has landed. A reply that
        // matches nothing — one for a request already completed or timed out, a duplicate, or
        // third-party traffic on a held channel — stays silent, as it always has.
        sentMessages.get(key).foreach(completeMatched(key, _, receivedTimestamp, message))
      }
      stay

    case ConsumerFailure(errorMessage) =>
      val now     = clock.nowMillis
      logger.error("Consumer failure propagated to tracker: {}", errorMessage)
      val pending = sentMessages.values.toList
      sentMessages.clear()
      for (p <- pending) {
        try
          executeNext(
            p.published.session.markAsFailed,
            p.startedAt,
            now,
            KO,
            p.published.next,
            p.published.requestName,
            None,
            Some(s"Consumer failure: $errorMessage"),
          )
        finally p.published.onComplete()
      }
      stay

    case TimeoutScan =>
      val now = clock.nowMillis
      sentMessages.valuesIterator.foreach { p =>
        val replyTimeout = p.published.replyTimeout
        if (replyTimeout > 0 && (now - p.startedAt) > replyTimeout) {
          timedOutMessages += p
        }
      }
      for (p <- timedOutMessages) {
        val mp       = p.published
        val matchKey = makeKeyForSentMessages(mp.matchId)
        logger.warn("Did not receive match for {} - key: {} after {}ms", describeBytes(mp.matchId), matchKey, mp.replyTimeout)
        sentMessages.remove(matchKey)
        try
          executeNext(
            mp.session.markAsFailed,
            p.startedAt,
            now,
            KO,
            mp.next,
            mp.requestName,
            None,
            Some(s"Reply timeout after ${mp.replyTimeout} ms"),
          )
        finally mp.onComplete()
      }
      timedOutMessages.clear()
      stay

    case Stop =>
      // Cancelling matters as much as dying: `die` only swaps the behaviour, so a scan left running would
      // keep firing TimeoutScan at a dead actor and keep it reachable — the leak would survive almost
      // intact. The pool only sends this once the channel has had nothing in flight for a full idle grace,
      // so there is no pending request left for the scan to time out.
      periodicTimeoutScan.foreach(_.cancel())
      periodicTimeoutScan = None
      die
  }

  private def executeNext(
      session: Session,
      sentTimestamp: Long,
      receivedTimestamp: Long,
      status: Status,
      next: Action,
      requestName: String,
      responseCode: Option[String],
      message: Option[String],
  ): Unit = {
    statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sentTimestamp,
      receivedTimestamp,
      status,
      responseCode,
      message,
    )
    next ! session.logGroupRequestTimings(sentTimestamp, receivedTimestamp)
  }

  /** Processes a matched message
    */
  private def processMessage(
      session: Session,
      sentTimestamp: Long,
      receivedTimestamp: Long,
      checks: List[KafkaCheck],
      message: KafkaProtocolMessage,
      next: Action,
      requestName: String,
  ): Unit = {
    val (newSession, error) = Check.check(message, session, checks)
    error match {
      case Some(Failure(errorMessage)) =>
        executeNext(
          newSession.markAsFailed,
          sentTimestamp,
          receivedTimestamp,
          KO,
          next,
          requestName,
          message.responseCode,
          Some(errorMessage),
        )
      case _                           =>
        executeNext(newSession, sentTimestamp, receivedTimestamp, OK, next, requestName, None, None)
    }
  }
}
