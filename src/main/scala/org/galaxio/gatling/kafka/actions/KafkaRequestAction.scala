package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session._
import io.gatling.core.session.el._
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

class KafkaRequestAction[K, V](
    val producer: KafkaProducer[K, V],
    val components: KafkaComponents,
    val attr: KafkaAttributes[K, V],
    val coreComponents: CoreComponents,
    val kafkaProtocol: KafkaProtocol,
    val throttled: Boolean,
    val next: Action,
) extends ExitableAction with NameGen {

  override val name: String    = genName("kafkaRequest")
  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock: Clock             = coreComponents.clock

  override def execute(session: Session): Unit = {
    recover(session) {
      val outcome = for {
        requestNameData <- attr.requestName(session)
        producerRecord  <- resolveProducerRecord(session)
      } yield coreComponents.throttler match {
        case Some(th) if throttled =>
          th ! Throttler.Command.ThrottledRequest(
            session.scenario,
            () => sendAndLogProducerRecord(requestNameData, producerRecord, session),
          )
        case _                     => sendAndLogProducerRecord(requestNameData, producerRecord, session)
      }

      outcome.onFailure { errorMessage => reportUnbuildableRequest(session, errorMessage) }

      outcome
    }

  }

  private def reportUnbuildableRequest(session: Session, error: String): Unit = {
    val loggedName = attr.requestName(session) match {
      case Success(requestNameValue) =>
        statsEngine.logRequestCrash(session.scenario, session.groups, requestNameValue, s"Failed to build request: $error")
        requestNameValue
      case _                         =>
        name
    }
    logger.error(s"'$loggedName' failed to execute: $error")
  }

  private def stringExpressionResolve[T](ek: Expression[T]): Expression[T] = s =>
    // need for work gatling Expression Language
    ek(s).flatMap {
      case key: String => key.el[String].apply(s).asInstanceOf[Validation[T]]
      case other       => other.success
    }

  private def resolveProducerRecord: Expression[ProducerRecord[K, V]] = s =>
    for {
      payload <- stringExpressionResolve(attr.payload)(s)
      key     <- attr.key.fold(null.asInstanceOf[K].success)(k => stringExpressionResolve(k)(s))
      headers <- attr.headers.fold(null.asInstanceOf[Headers].success)(_(s))
    } yield new ProducerRecord[K, V](
      kafkaProtocol.producerTopic,
      null,
      key,
      payload,
      headers,
    )

  private def sendAndLogProducerRecord(
      requestName: String,
      record: ProducerRecord[K, V],
      session: Session,
  ): Unit = {
    val requestStartDate = clock.nowMillis
    scala.concurrent.blocking(
      scala.concurrent
        .Future(producer.send(record).get())(components.sender.executionContext)
        .onComplete {
          case util.Success(rm) =>
            val requestEndDate = clock.nowMillis
            if (logger.underlying.isDebugEnabled) {
              logger.debug(s"Record sent user=${session.userId} key=${record.key} topic=${rm.topic()}")
              logger.trace(s"ProducerRecord=$record")
            }

            statsEngine.logResponse(
              session.scenario,
              session.groups,
              requestName,
              startTimestamp = requestStartDate,
              endTimestamp = requestEndDate,
              OK,
              None,
              None,
            )
            next ! session.logGroupRequestTimings(requestStartDate, requestEndDate)

          case util.Failure(exception) =>
            val requestEndDate = clock.nowMillis

            logger.error(exception.getMessage, exception)

            statsEngine.logResponse(
              session.scenario,
              session.groups,
              requestName,
              startTimestamp = requestStartDate,
              endTimestamp = requestEndDate,
              KO,
              None,
              Some(exception.getMessage),
            )
            next ! session.logGroupRequestTimings(requestStartDate, requestEndDate).markAsFailed
        }(components.sender.executionContext),
    )
  }
}
