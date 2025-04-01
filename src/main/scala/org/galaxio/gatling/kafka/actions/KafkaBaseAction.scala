package org.galaxio.gatling.kafka.actions

import com.sksamuel.avro4s
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.validation.{Success, SuccessWrapper, Validation}
import io.gatling.core.action.RequestAction
import io.gatling.core.session.el._
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaSender

abstract class KafkaBaseAction[K, V] extends RequestAction with KafkaLogging with NameGen {

  def optToVal[T](ovt: Option[Validation[T]]): Validation[Option[T]] =
    ovt.fold(Option.empty[T].success)(_.map(Option[T]))

  def stringExpressionResolve[T](ek: Expression[T]): Expression[T] = s =>
    // need for work gatling Expression Language
    ek(s).flatMap {
      case key: String => key.el[String].apply(s).asInstanceOf[Validation[T]]
      case other       => other.success
    }

  def serializeKey(
      serializer: Serializer[K],
      keyE: Expression[K],
      topicE: Expression[String],
  ): Expression[Array[Byte]] = s =>
      for {
        topic <- topicE(s)
        key   <- keyE(s)
      } yield serializer.serialize(topic, key)

  def reportUnbuildableRequest(requestName: Expression[String], session: Session, error: String): Unit = {
    val loggedName = requestName(session) match {
      case Success(requestNameValue) =>
        statsEngine.logRequestCrash(session.scenario, session.groups, requestNameValue, s"Failed to build request: $error")
        requestNameValue
      case _                         =>
        name
    }
    logger.error(s"'$loggedName' failed to execute: $error")
  }

  /** Publish a record to Kafka
    *
    * @param kafkaSender
    *   The sender for this record (generics for types)
    * @param requestName
    *   The name/testName of this sender component
    * @param message
    *   The message/event to be published
    * @param session
    *   Reference to the session this event send is for
    */
  def sendAndLogProducerRecord(
      kafkaSender: KafkaSender[K, V],
      requestName: String,
      message: ProducerRecord[K, V],
      session: Session,
  ): Unit = {
    val requestStartDate = clock.nowMillis
      kafkaSender.send(
        protocolMessage = message,
        onSuccess = _ => successCaseGeneric(requestName, message, session, requestStartDate),
        onFailure = exception => failureCase(requestName, session, requestStartDate, exception),
      )
  }

  /** Publish a record to Kafka
    *
    * @param kafkaSender
    *   The sender for this record (specific for avro records)
    * @param requestName
    *   The name/testName of this sender component
    * @param message
    *   The message/event to be published
    * @param session
    *   Reference to the session this event send is for
    */
  def sendAndLogAvroRecord(
      kafkaSender: KafkaSender[K, avro4s.Record],
      requestName: String,
      message: ProducerRecord[K, avro4s.Record],
      session: Session,
  ): Unit = {
    logger.info("Sending Avro Record")
    val requestStartDate = clock.nowMillis
    scala.concurrent.blocking(
      kafkaSender.send(
        protocolMessage = message,
        onSuccess = _ => successCaseAvro(requestName, message, session, requestStartDate),
        onFailure = exception => failureCase(requestName, session, requestStartDate, exception),
      ),
    )
  }

  private def successCaseAvro(
      requestName: String,
      message: ProducerRecord[K, avro4s.Record],
      session: Session,
      requestStartDate: Long,
  ): Unit = {
    val requestEndDate = clock.nowMillis
    if (logger.underlying.isDebugEnabled) {
      logger.debug("Record sent user={} key={} topic={}", session.userId, message.key, message.topic)
      logger.trace("ProducerRecord={}", message.value)
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
  }

  private def failureCase(requestName: String, session: Session, requestStartDate: Long, exception: Throwable): Unit = {

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
  }

  private def successCaseGeneric(
      requestName: String,
      message: ProducerRecord[K, V],
      session: Session,
      requestStartDate: Long,
  ): Unit = {
    val requestEndDate = clock.nowMillis
    if (logger.underlying.isDebugEnabled) {
      logger.debug("Record sent user={} key={} topic={}", session.userId, message.key, message.topic)
      logger.trace("ProducerRecord={}", message.value)
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
  }

}
