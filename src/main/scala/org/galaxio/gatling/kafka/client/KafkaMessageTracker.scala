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

  final case class ConsumerFailure(errorMessage: String) extends TrackerMessage

  /** Asks the tracker to cancel its periodic timeout scan and stop.
    *
    * Sent by [[KafkaMessageTrackerPool]] once the reply channel this tracker belongs to has been released. It exists because
    * Gatling's `ActorSystem` offers no way to stop an actor from outside — `die` is an `Effect` reachable only from the actor's
    * own behaviour — so stopping has to be asked for (issue #166).
    */
  private[client] final case object Stop extends TrackerMessage

  private final case object TimeoutScan extends TrackerMessage

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

  private val sentMessages     = mutable.HashMap.empty[String, MessagePublished]
  private val timedOutMessages = mutable.ArrayBuffer.empty[MessagePublished]

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

  override def init(): Behavior[TrackerMessage] = {
    // message was sent; add the timestamps to the map
    case messageSent: MessagePublished =>
      val key = makeKeyForSentMessages(messageSent.matchId)
      logger.debug("Published with MatchId: {} Tracking Key: {}", describeBytes(messageSent.matchId), key)
      sentMessages += key -> messageSent
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
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
        sentMessages.remove(key).foreach {
          case MessagePublished(_, sentTimestamp, _, checks, session, next, requestName, onComplete) =>
            try processMessage(session, sentTimestamp, receivedTimestamp, checks, message, next, requestName)
            finally onComplete()
        }
      }
      stay

    case ConsumerFailure(errorMessage) =>
      val now     = clock.nowMillis
      logger.error("Consumer failure propagated to tracker: {}", errorMessage)
      val pending = sentMessages.values.toList
      sentMessages.clear()
      for (mp <- pending) {
        try
          executeNext(
            mp.session.markAsFailed,
            mp.sentTimestamp,
            now,
            KO,
            mp.next,
            mp.requestName,
            None,
            Some(s"Consumer failure: $errorMessage"),
          )
        finally mp.onComplete()
      }
      stay

    case TimeoutScan =>
      val now = clock.nowMillis
      sentMessages.valuesIterator.foreach { messagePublished =>
        val replyTimeout = messagePublished.replyTimeout
        if (replyTimeout > 0 && (now - messagePublished.sentTimestamp) > replyTimeout) {
          timedOutMessages += messagePublished
        }
      }
      for (mp <- timedOutMessages) {
        val matchKey = makeKeyForSentMessages(mp.matchId)
        logger.warn("Did not receive match for {} - key: {} after {}ms", describeBytes(mp.matchId), matchKey, mp.replyTimeout)
        sentMessages.remove(matchKey)
        try
          executeNext(
            mp.session.markAsFailed,
            mp.sentTimestamp,
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
