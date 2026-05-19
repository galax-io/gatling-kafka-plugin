package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.{Success, Validation}
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

object KafkaProduceActionBase {
  private[actions] def nextSession(session: Session, exception: Exception): Session =
    if (exception == null) session else session.markAsFailed
}

abstract class KafkaProduceActionBase[K, V, P](
    val producer: KafkaProducer[K, P],
    val coreComponents: CoreComponents,
    val kafkaProtocol: KafkaProtocol,
    val throttled: Boolean,
    val next: Action,
) extends ExitableAction with NameGen {

  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock: Clock             = coreComponents.clock

  protected def requestNameExpr: Expression[String]
  protected def keyExpr: Option[Expression[K]]
  protected def payloadExpr: Expression[V]
  protected def headersExpr: Option[Expression[Headers]]
  protected def partitionExpr: Option[Expression[java.lang.Integer]] = None
  protected def timestampExpr: Option[Expression[java.lang.Long]]    = None
  protected def isSilent: Boolean
  protected def transformPayload(v: V): P

  override def execute(session: Session): Unit = recover(session) {
    requestNameExpr(session).flatMap { requestName =>
      val outcome = sendRequest(requestName, session)
      outcome.onFailure { errorMessage =>
        logger.error(errorMessage)
        if (!isSilent)
          statsEngine.logRequestCrash(session.scenario, session.groups, requestName, s"Failed to build request: $errorMessage")
      }
      outcome
    }
  }

  private def sendRequest(requestName: String, session: Session): Validation[Unit] =
    for {
      payload   <- payloadExpr(session)
      key       <- keyExpr.map(_(session)).getOrElse(Success(null.asInstanceOf[K]))
      headers   <- headersExpr.map(_(session)).getOrElse(Success(null.asInstanceOf[Headers]))
      partition <- partitionExpr.map(_(session)).getOrElse(Success(null.asInstanceOf[java.lang.Integer]))
      timestamp <- timestampExpr.map(_(session)).getOrElse(Success(null.asInstanceOf[java.lang.Long]))
    } yield {
      val record = new ProducerRecord[K, P](
        kafkaProtocol.producerTopic,
        partition,
        timestamp,
        key,
        transformPayload(payload),
        headers,
      )

      val requestStartDate = clock.nowMillis

      producer.send(
        record,
        (_: RecordMetadata, e: Exception) => {
          val requestEndDate = clock.nowMillis

          if (!isSilent) {
            statsEngine.logResponse(
              session.scenario,
              session.groups,
              requestName,
              startTimestamp = requestStartDate,
              endTimestamp = requestEndDate,
              if (e == null) OK else KO,
              None,
              if (e == null) None else Some(e.getMessage),
            )
          }

          val sessionAfterSend = if (e == null) session else session.markAsFailed

          coreComponents.throttler match {
            case Some(th) if throttled =>
              th ! Throttler.Command.ThrottledRequest(session.scenario, () => next ! sessionAfterSend)
            case _                     => next ! sessionAfterSend
          }
        },
      )
    }
}
