package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

class KafkaAvro4sRequestAction[K, V](
    val producer: KafkaProducer[K, GenericRecord],
    val attr: Avro4sAttributes[K, V],
    val coreComponents: CoreComponents,
    val kafkaProtocol: KafkaProtocol,
    val throttled: Boolean,
    val next: Action,
) extends ExitableAction with NameGen {

  val statsEngine: StatsEngine = coreComponents.statsEngine
  val clock                    = new DefaultClock
  override val name: String    = genName("kafkaAvroRequest")

  override def execute(session: Session): Unit = recover(session) {
    attr requestName session flatMap { requestName =>
      val outcome = sendRequest(requestName, producer, attr, throttled, session)

      outcome.onFailure(errorMessage => {
        logger.error(errorMessage)
        statsEngine.logRequestCrash(session.scenario, session.groups, requestName, s"Failed to build request: $errorMessage")
      })

      outcome
    }
  }

  def sendRequest(
      requestName: String,
      producer: KafkaProducer[K, GenericRecord],
      attr: Avro4sAttributes[K, V],
      throttled: Boolean,
      session: Session,
  ): Validation[Unit] = {

    attr payload session map { payload =>
      val headers = attr.headers
        .map(h => h(session).toOption.get)
        .orNull
      val key     = attr.key
        .map(k => k(session).toOption.get)
        .getOrElse(null.asInstanceOf[K])

      val record: ProducerRecord[K, GenericRecord] =
        new ProducerRecord(kafkaProtocol.producerTopic, null, key, attr.format.to(payload), headers)

      val requestStartDate = clock.nowMillis

      producer.send(
        record,
        (_: RecordMetadata, e: Exception) => {

          val requestEndDate = clock.nowMillis
          statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestName,
            requestStartDate,
            requestEndDate,
            if (e == null) OK else KO,
            None,
            if (e == null) None else Some(e.getMessage),
          )

          coreComponents.throttler match {
            case Some(th) if throttled => th ! Throttler.Command.ThrottledRequest(session.scenario, () => next ! session)
            case _                     => next ! session
          }
        },
      )
    }
  }
}
