package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session._
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.producer._
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

object KafkaRequestAction {
  private[actions] def nextSession(session: Session, exception: Exception): Session =
    if (exception == null) session else session.markAsFailed
}

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
  val clock                    = new DefaultClock

  override def execute(session: Session): Unit = recover(session) {

    attr requestName session flatMap { requestName =>
      val outcome =
        sendRequest(requestName, producer, attr, throttled, session)

      outcome.onFailure(errorMessage => {
        logger.error(errorMessage)
        if (!attr.silent.getOrElse(false))
          statsEngine.logRequestCrash(session.scenario, session.groups, requestName, s"Failed to build request: $errorMessage")
      })

      outcome

    }

  }

  private def sendRequest(
      requestName: String,
      producer: Producer[K, V],
      kafkaAttributes: KafkaAttributes[K, V],
      throttled: Boolean,
      session: Session,
  ): Validation[Unit] = {

    kafkaAttributes payload session map { payload =>
      val key = kafkaAttributes.key
        .map(k => k(session).toOption.get)
        .getOrElse(null.asInstanceOf[K])

      val headers = kafkaAttributes.headers
        .map(h => h(session).toOption.get)
        .orNull

      val record = new ProducerRecord[K, V](
        kafkaProtocol.producerTopic,
        null,
        key,
        payload,
        headers,
      )

      val requestStartDate = clock.nowMillis
      val silent           = kafkaAttributes.silent.getOrElse(false)

      producer.send(
        record,
        (_: RecordMetadata, e: Exception) => {

          val requestEndDate = clock.nowMillis

          if (!silent) {
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

          coreComponents.throttler match {
            case Some(th) if throttled =>
              val sessionAfterSend = KafkaRequestAction.nextSession(session, e)
              th.throttle(session.scenario, () => next ! sessionAfterSend)
            case _                     =>
              next ! KafkaRequestAction.nextSession(session, e)
          }

        },
      )

    }

  }

}
