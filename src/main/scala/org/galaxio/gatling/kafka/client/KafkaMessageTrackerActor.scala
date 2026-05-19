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
import org.galaxio.gatling.kafka.request.builder.KafkaReplyExtraction

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

object KafkaMessageTrackerActor {
  private val BufferedReplyRetentionMillis = 60_000L

  sealed trait TrackerMessage

  final case class MessagePublished(
      matchId: Array[Byte],
      sent: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      replyExtractions: List[KafkaReplyExtraction],
      session: Session,
      next: Action,
      requestName: String,
      silentRequest: Boolean,
  ) extends TrackerMessage

  final case class MessageConsumed(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
  ) extends TrackerMessage

  private[client] final case class BufferedReply(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
  )

  case object TimeoutScan extends TrackerMessage

  private[client] def makeTrackingKey(m: Array[Byte]): String =
    Option(m).map(java.util.Base64.getEncoder.encodeToString).getOrElse("")

  private[client] def timedOutMessages(
      now: Long,
      sentMessages: mutable.HashMap[String, mutable.Queue[MessagePublished]],
  ): List[MessagePublished] =
    sentMessages.valuesIterator
      .flatMap(_.iterator)
      .filter { messagePublished =>
        val replyTimeout = messagePublished.replyTimeout
        replyTimeout > 0 && (now - messagePublished.sent) > replyTimeout
      }
      .toList

  private[client] def removeTrackedMessage(
      matchId: Array[Byte],
      sentMessages: mutable.HashMap[String, mutable.Queue[MessagePublished]],
  ): Option[MessagePublished] = {
    val key = makeTrackingKey(matchId)
    sentMessages.get(key).flatMap { queue =>
      if (queue.nonEmpty) {
        val published = queue.dequeue()
        if (queue.isEmpty) {
          sentMessages.remove(key)
        }
        Some(published)
      } else {
        sentMessages.remove(key)
        None
      }
    }
  }

  private[client] def removeTimedOutMessages(
      timedOutMessages: List[MessagePublished],
      sentMessages: mutable.HashMap[String, mutable.Queue[MessagePublished]],
  ): List[MessagePublished] =
    timedOutMessages.filter { message =>
      val key = makeTrackingKey(message.matchId)
      sentMessages.get(key).exists { queue =>
        val index = queue.indexWhere(_ eq message)
        if (index >= 0) {
          queue.remove(index)
          if (queue.isEmpty) {
            sentMessages.remove(key)
          }
          true
        } else {
          false
        }
      }
    }

  private[client] def bufferReply(
      replyId: Array[Byte],
      received: Long,
      message: KafkaProtocolMessage,
      bufferedReplies: mutable.HashMap[String, mutable.Queue[BufferedReply]],
  ): Unit =
    bufferedReplies.getOrElseUpdate(makeTrackingKey(replyId), mutable.Queue.empty) += BufferedReply(replyId, received, message)

  private[client] def removeBufferedReply(
      matchId: Array[Byte],
      bufferedReplies: mutable.HashMap[String, mutable.Queue[BufferedReply]],
  ): Option[BufferedReply] = {
    val key = makeTrackingKey(matchId)
    bufferedReplies.get(key).flatMap { queue =>
      if (queue.nonEmpty) {
        val reply = queue.dequeue()
        if (queue.isEmpty) {
          bufferedReplies.remove(key)
        }
        Some(reply)
      } else {
        bufferedReplies.remove(key)
        None
      }
    }
  }

  private[client] def removeExpiredBufferedReplies(
      now: Long,
      bufferedReplies: mutable.HashMap[String, mutable.Queue[BufferedReply]],
      retentionMillis: Long = BufferedReplyRetentionMillis,
  ): Unit =
    bufferedReplies.filterInPlace { (_, replies) =>
      val freshReplies = replies.filter(reply => (now - reply.received) <= retentionMillis)
      replies.clear()
      replies ++= freshReplies
      replies.nonEmpty
    }

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

class KafkaMessageTrackerActor(name: String, statsEngine: StatsEngine, clock: Clock)
    extends Actor[KafkaMessageTrackerActor.TrackerMessage](name) {
  import KafkaMessageTrackerActor._

  private val sentMessages                 = mutable.HashMap.empty[String, mutable.Queue[MessagePublished]]
  private val timedOutMessages             = mutable.ArrayBuffer.empty[MessagePublished]
  private val bufferedReplies              = mutable.HashMap.empty[String, mutable.Queue[BufferedReply]]
  private var periodicTimeoutScanTriggered = false

  private def triggerPeriodicTimeoutScan(): Unit =
    if (!periodicTimeoutScanTriggered && (sentMessages.nonEmpty || bufferedReplies.nonEmpty)) {
      periodicTimeoutScanTriggered = true
      scheduler.scheduleAtFixedRate(1000.millis) {
        self ! TimeoutScan
      }
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

  override def init(): Behavior[TrackerMessage] = {
    case messageSent: MessagePublished =>
      removeBufferedReply(messageSent.matchId, this.bufferedReplies) match {
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
          this.sentMessages.getOrElseUpdate(key, mutable.Queue.empty) += messageSent
          if (messageSent.replyTimeout > 0 || this.bufferedReplies.nonEmpty) {
            triggerPeriodicTimeoutScan()
          }
      }
      stay

    case MessageConsumed(replyId, received, message) =>
      removeTrackedMessage(replyId, this.sentMessages) match {
        case Some(MessagePublished(_, sent, _, checks, replyExtractions, session, next, requestName, silentRequest)) =>
          processMessage(session, sent, received, checks, replyExtractions, message, next, requestName, silentRequest)
        case None                                                                                                    =>
          bufferReply(replyId, received, message, this.bufferedReplies)
          triggerPeriodicTimeoutScan()
      }
      stay

    case TimeoutScan =>
      val now = clock.nowMillis
      removeExpiredBufferedReplies(now, this.bufferedReplies)
      timedOutMessages ++= KafkaMessageTrackerActor.timedOutMessages(now, this.sentMessages)
      for (
        MessagePublished(matchId, sent, receivedTimeout, _, _, session, next, requestName, silentRequest) <-
          removeTimedOutMessages(timedOutMessages.toList, this.sentMessages)
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
      stay
  }
}
