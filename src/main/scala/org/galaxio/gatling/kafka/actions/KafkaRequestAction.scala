package org.galaxio.gatling.kafka.actions

import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session._
import io.gatling.core.stats.StatsEngine
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.client.{KafkaSender, KafkaSenderImpl}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

class KafkaRequestAction[K, V](
    val components: KafkaComponents,
    val attributes: KafkaAttributes[K, V],
    val kafkaProtocol: KafkaProtocol,
    val next: Action,
    val throttler: Option[ActorRef[Throttler.Command]],
) extends KafkaBaseAction[K, V] {

  val statsEngine: StatsEngine                 = components.coreComponents.statsEngine
  val clock: Clock                             = components.coreComponents.clock
  override def requestName: Expression[String] = attributes.requestName
  override val name: String                    = genName("kafkaRequest")

  private val kafkaSender: KafkaSender[K, V] = new KafkaSenderImpl(kafkaProtocol.producerProperties, components.coreComponents)

  override def sendRequest(session: Session): Validation[Unit] = for {
    requestNameData <- attributes.requestName(session)
    producerRecord  <- resolveProducerRecord(session)
  } yield throttler match {
    case Some(th) =>
      th ! Throttler.Command.ThrottledRequest(
        session.scenario,
        () => sendAndLogProducerRecord(kafkaSender, requestNameData, producerRecord, session),
      )
    case _        => sendAndLogProducerRecord(kafkaSender, requestNameData, producerRecord, session)
  }

  private def resolveProducerRecord: Expression[ProducerRecord[K, V]] = s =>
    for {
      payload <- stringExpressionResolve(attributes.value)(s)
      key     <- attributes.key.fold(null.asInstanceOf[K].success)(k => stringExpressionResolve(k)(s))
      headers <- attributes.headers.fold(null.asInstanceOf[Headers].success)(_(s))
    } yield new ProducerRecord[K, V](
      kafkaProtocol.producerTopic,
      null,
      key,
      payload,
      headers,
    )
}
