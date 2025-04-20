package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, Behavior}
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object KafkaMessageTrackerActor {

  sealed trait KafkaMessage

  final case class MessagePublished(
      matchId: Array[Byte],
      sent: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      session: Session,
      next: Action,
      requestName: String,
  ) extends KafkaMessage

  final case class MessageConsumed(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
  ) extends KafkaMessage

  case object TimeoutScan extends KafkaMessage

  private def makeKeyForSentMessages(m: Array[Byte]): String =
    Option(m).map(java.util.Base64.getEncoder.encodeToString).getOrElse("")
}

class KafkaMessageTrackerActor(name: String, statsEngine: StatsEngine, clock: Clock)
    extends Actor[KafkaMessageTrackerActor.KafkaMessage](name) {
  import KafkaMessageTrackerActor._

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

  override def init(): Behavior[KafkaMessageTrackerActor.KafkaMessage] = {
    // message was sent; add the timestamps to the map
    case messageSent: MessagePublished               =>
      val key = makeKeyForSentMessages(messageSent.matchId)
      sentMessages += key -> messageSent
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
      }
      stay

    // message was received; publish stats and remove from the map
    case MessageConsumed(replyId, received, message) =>
      // if key is missing, message was already acked and is a dup, or request timeout
      val key = makeKeyForSentMessages(replyId)
      sentMessages.remove(key).foreach { case MessagePublished(_, sent, _, checks, session, next, requestName) =>
        processMessage(session, sent, received, checks, message, next, requestName)
      }
      stay

    case TimeoutScan =>
      val now = clock.nowMillis
      sentMessages.valuesIterator.foreach { messagePublished =>
        val replyTimeout = messagePublished.replyTimeout
        if (replyTimeout > 0 && (now - messagePublished.sent) > replyTimeout) {
          timedOutMessages += messagePublished
        }
      }
      for (MessagePublished(matchId, sent, receivedTimeout, _, session, next, requestName) <- timedOutMessages) {
        sentMessages.remove(makeKeyForSentMessages(matchId))
        executeNext(
          session.markAsFailed,
          sent,
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
      sent: Long,
      received: Long,
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
      sent,
      received,
      status,
      responseCode,
      message,
    )
    next ! session.logGroupRequestTimings(sent, received)
  }

  /** Processes a matched message
    */
  private def processMessage(
      session: Session,
      sent: Long,
      received: Long,
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
          sent,
          received,
          KO,
          next,
          requestName,
          message.responseCode,
          Some(errorMessage),
        )
      case _                           =>
        executeNext(newSession, sent, received, OK, next, requestName, None, None)
    }
  }
}
