package org.galaxio.gatling.kafka.client

import akka.actor.{Actor, Props, Timers}
import com.typesafe.scalalogging.LazyLogging
import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaReplyExtraction

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

object KafkaMessageTrackerActor {
  private val BufferedReplyRetentionMillis = 60_000L

  def props(statsEngine: StatsEngine, clock: Clock): Props =
    Props(new KafkaMessageTrackerActor(statsEngine, clock))

  case class MessagePublished(
      matchId: Array[Byte],
      sent: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      replyExtractions: List[KafkaReplyExtraction],
      session: Session,
      next: Action,
      requestName: String,
      silentRequest: Boolean,
  )

  case class MessageConsumed(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
  )

  private[client] final case class BufferedReply(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
  )

  case object TimeoutScan

  private[client] def makeTrackingKey(m: Array[Byte]): String =
    Option(m).map(java.util.Base64.getEncoder.encodeToString).getOrElse("")

  private[client] def timedOutMessages(
      now: Long,
      sentMessages: mutable.HashMap[String, MessagePublished],
  ): List[MessagePublished] =
    sentMessages.valuesIterator.filter { messagePublished =>
      val replyTimeout = messagePublished.replyTimeout
      replyTimeout > 0 && (now - messagePublished.sent) > replyTimeout
    }.toList

  private[client] def removeTrackedMessage(
      matchId: Array[Byte],
      sentMessages: mutable.HashMap[String, MessagePublished],
  ): Option[MessagePublished] =
    sentMessages.remove(makeTrackingKey(matchId))

  private[client] def removeTimedOutMessages(
      timedOutMessages: List[MessagePublished],
      sentMessages: mutable.HashMap[String, MessagePublished],
  ): List[MessagePublished] =
    timedOutMessages.filter(message => removeTrackedMessage(message.matchId, sentMessages).isDefined)

  private[client] def bufferReply(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
      bufferedReplies: mutable.HashMap[String, BufferedReply],
  ): Unit =
    bufferedReplies.update(makeTrackingKey(replyId), BufferedReply(replyId, received, message))

  private[client] def removeBufferedReply(
      matchId: Array[Byte],
      bufferedReplies: mutable.HashMap[String, BufferedReply],
  ): Option[BufferedReply] =
    bufferedReplies.remove(makeTrackingKey(matchId))

  private[client] def removeExpiredBufferedReplies(
      now: Long,
      bufferedReplies: mutable.HashMap[String, BufferedReply],
      retentionMillis: Long = BufferedReplyRetentionMillis,
  ): Unit =
    bufferedReplies.filterInPlace((_, bufferedReply) => (now - bufferedReply.received) <= retentionMillis)

  private[client] def applyReplyExtractions(
      session: Session,
      message: KafkaProtocolMessage,
      replyExtractions: List[KafkaReplyExtraction],
  ): Either[String, Session] =
    replyExtractions.foldLeft[Either[String, Session]](Right(session)) { case (acc, extraction) =>
      acc.flatMap { currentSession =>
        Try(extraction.extractor(message)).toEither.left.map(_.getMessage).flatMap { extractedValue =>
          Option(extractedValue)
            .toRight(s"reply extraction '${extraction.sessionKey}' returned null")
            .map(currentSession.set(extraction.sessionKey, _))
        }
      }
    }
}

class KafkaMessageTrackerActor(statsEngine: StatsEngine, clock: Clock) extends Actor with Timers with LazyLogging {
  import KafkaMessageTrackerActor._
  def triggerPeriodicTimeoutScan(
      periodicTimeoutScanTriggered: Boolean,
      sentMessages: mutable.HashMap[String, MessagePublished],
      timedOutMessages: mutable.ArrayBuffer[MessagePublished],
      bufferedReplies: mutable.HashMap[String, BufferedReply],
  ): Unit =
    if (!periodicTimeoutScanTriggered && (sentMessages.nonEmpty || bufferedReplies.nonEmpty)) {
      context.become(onMessage(periodicTimeoutScanTriggered = true, sentMessages, timedOutMessages, bufferedReplies))
      timers.startTimerWithFixedDelay("timeoutTimer", TimeoutScan, 1000.millis)
    }

  override def receive: Receive =
    onMessage(
      periodicTimeoutScanTriggered = false,
      mutable.HashMap.empty[String, MessagePublished],
      mutable.ArrayBuffer.empty[MessagePublished],
      mutable.HashMap.empty[String, BufferedReply],
    )

  private def executeNext(
      session: Session,
      sent: Long,
      received: Long,
      status: Status,
      next: Action,
      requestName: String,
      responseCode: Option[String],
      message: Option[String],
      silentRequest: Boolean,
  ): Unit = {
    if (!silentRequest) {
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
    } else
      next ! session
  }

  /** Processes a matched message
    */
  private def processMessage(
      session: Session,
      sent: Long,
      received: Long,
      checks: List[KafkaCheck],
      replyExtractions: List[KafkaReplyExtraction],
      message: KafkaProtocolMessage,
      next: Action,
      requestName: String,
      silentRequest: Boolean,
  ): Unit = {
    val (checkedSession, error) = Check.check(message, session, checks)
    error match {
      case Some(Failure(errorMessage)) =>
        executeNext(
          checkedSession.markAsFailed,
          sent,
          received,
          KO,
          next,
          requestName,
          message.responseCode,
          Some(errorMessage),
          silentRequest,
        )
      case _                           =>
        KafkaMessageTrackerActor.applyReplyExtractions(checkedSession, message, replyExtractions) match {
          case Left(errorMessage) =>
            executeNext(
              checkedSession.markAsFailed,
              sent,
              received,
              KO,
              next,
              requestName,
              message.responseCode,
              Some(errorMessage),
              silentRequest,
            )
          case Right(newSession)  =>
            executeNext(newSession, sent, received, OK, next, requestName, None, None, silentRequest)
        }
    }
  }

  private def onMessage(
      periodicTimeoutScanTriggered: Boolean,
      sentMessages: mutable.HashMap[String, MessagePublished],
      timedOutMessages: mutable.ArrayBuffer[MessagePublished],
      bufferedReplies: mutable.HashMap[String, BufferedReply],
  ): Receive = {
    // message was sent; add the timestamps to the map
    case messageSent: MessagePublished =>
      removeBufferedReply(messageSent.matchId, bufferedReplies) match {
        case Some(BufferedReply(_, received, message)) =>
          processMessage(
            messageSent.session,
            messageSent.sent,
            received,
            messageSent.checks,
            messageSent.replyExtractions,
            message,
            messageSent.next,
            messageSent.requestName,
            messageSent.silentRequest,
          )
        case None                                      =>
          val key = makeTrackingKey(messageSent.matchId)
          sentMessages += key -> messageSent
          if (messageSent.replyTimeout > 0 || bufferedReplies.nonEmpty) {
            triggerPeriodicTimeoutScan(periodicTimeoutScanTriggered, sentMessages, timedOutMessages, bufferedReplies)
          }
      }

    // message was received; publish stats and remove from the map
    case MessageConsumed(replyId, received, message) =>
      // if key is missing, message was already acked and is a dup, or request timeout
      removeTrackedMessage(replyId, sentMessages) match {
        case Some(MessagePublished(_, sent, _, checks, replyExtractions, session, next, requestName, silentRequest)) =>
          processMessage(session, sent, received, checks, replyExtractions, message, next, requestName, silentRequest)
        case None                                                                                                    =>
          bufferReply(replyId, received, message, bufferedReplies)
          triggerPeriodicTimeoutScan(periodicTimeoutScanTriggered, sentMessages, timedOutMessages, bufferedReplies)
      }

    case TimeoutScan =>
      val now = clock.nowMillis
      removeExpiredBufferedReplies(now, bufferedReplies)
      timedOutMessages ++= KafkaMessageTrackerActor.timedOutMessages(now, sentMessages)
      for (
        MessagePublished(matchId, sent, receivedTimeout, _, _, session, next, requestName, silentRequest) <-
          removeTimedOutMessages(timedOutMessages.toList, sentMessages)
      ) {
        executeNext(
          session.markAsFailed,
          sent,
          now,
          KO,
          next,
          requestName,
          None,
          Some(s"Reply timeout after $receivedTimeout ms"),
          silentRequest,
        )
      }
      timedOutMessages.clear()
  }
}
