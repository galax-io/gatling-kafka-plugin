package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.Session
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

class KafkaAvro4sRequestAction[K, V](
    val producer: KafkaProducer[K, GenericRecord],
    val components: KafkaComponents,
    val attr: Avro4sAttributes[K, V],
    val coreComponents: CoreComponents,
    val kafkaProtocol: KafkaProtocol,
    val throttled: Boolean,
    val next: Action,
) extends ExitableAction with NameGen {

  override val name: String    = genName("kafkaAvroRequest")
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

        case _ => sendAndLogProducerRecord(requestNameData, producerRecord, session)
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

  private def resolveProducerRecord: Expression[ProducerRecord[K, GenericRecord]] = s =>
    for {
      payload <- attr.payload(s).flatMap(pl => scala.util.Try(attr.format.to(pl)).toValidation)
      key     <- attr.key.fold(null.asInstanceOf[K].success)(_(s))
      headers <- attr.headers.fold(null.asInstanceOf[Headers].success)(_(s))
    } yield new ProducerRecord[K, GenericRecord](
      kafkaProtocol.producerTopic,
      null,
      key,
      payload,
      headers,
    )

  private def sendAndLogProducerRecord(
      requestName: String,
      record: ProducerRecord[K, GenericRecord],
      session: Session,
  ): Unit = {
    val requestStartDate = clock.nowMillis
    scala.concurrent
      .Future(scala.concurrent.blocking(producer.send(record).get()))(components.sender.executionContext)
      .onComplete {
        case util.Success(rm) =>
          val requestEndDate = clock.nowMillis
          if (logger.underlying.isDebugEnabled) {
            logger.debug(s"Avro record sent user=${session.userId} key=${record.key} topic=${rm.topic()}")
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
      }(components.sender.executionContext)
  }
}
