package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, Behavior}
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
  ) extends TrackerMessage

  final case class MessageConsumed(
      received: Long,
      message: KafkaProtocolMessage,
  ) extends TrackerMessage

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

  private val sentMessages                 = mutable.HashMap.empty[String, MessagePublished]
  private val timedOutMessages             = mutable.ArrayBuffer.empty[MessagePublished]
  private var periodicTimeoutScanTriggered = false

  private def triggerPeriodicTimeoutScan(): Unit =
    if (!periodicTimeoutScanTriggered) {
      periodicTimeoutScanTriggered = true
      scheduler.scheduleAtFixedRate(1000.millis) {
        self ! TimeoutScan
      }
    }

  override def init(): Behavior[TrackerMessage] = {
    // message was sent; add the timestamps to the map
    case messageSent: MessagePublished =>
      val key = makeKeyForSentMessages(messageSent.matchId)
      logger.debug("Published with MatchId: {} Tracking Key: {}", new String(messageSent.matchId), key)
      sentMessages += key -> messageSent
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
      }
      stay

    // message was received; publish stats and remove from the map
    case MessageConsumed(receivedTimestamp, forTransformMessage) =>
      val message = responseTransformer.map(_(forTransformMessage)).getOrElse(forTransformMessage)
      if (messageMatcher.responseMatch(message) == null) {
        logger.error("no messageMatcher key for read message {}", message.key)
      } else {
        if (message.key == null || message.value == null) {
          logger.warn(" --- received message with null key or value")
        } else {
          logger.trace(" --- received {} {}", message.key, message.value)
        }

        val replyId    = messageMatcher.responseMatch(message)
        val messageKey = if (message.key == null) "null" else new String(message.key)
        logMessage(s"Record received key=$messageKey", message)
        // if key is missing, message was already acked and is a dup, or request timeout
        val key        = makeKeyForSentMessages(replyId)
        logger.debug(
          "Received with MatchId: {} Tracking Key: {}, producerTopic: {}, consumerTopic: {}",
          new String(replyId),
          key,
          message.producerTopic,
          message.consumerTopic,
        )
        sentMessages.remove(key).foreach { case MessagePublished(_, sentTimestamp, _, checks, session, next, requestName) =>
          processMessage(session, sentTimestamp, receivedTimestamp, checks, message, next, requestName)
        }
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
      for (MessagePublished(matchId, sentTimestamp, receivedTimeout, _, session, next, requestName) <- timedOutMessages) {
        val matchKey = makeKeyForSentMessages(matchId)
        logger.warn("Did not receive match for {} - key: {} after {}ms", new String(matchId), matchKey, receivedTimeout)
        sentMessages.remove(matchKey)
        executeNext(
          session.markAsFailed,
          sentTimestamp,
          now,
          KO,
          next,
          requestName,
          None,
          Some(s"Reply timeout after $receivedTimeout ms"),
        )
      }
      timedOutMessages.clear()
      stay
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
