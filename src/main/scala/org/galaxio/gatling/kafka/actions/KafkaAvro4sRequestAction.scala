package org.galaxio.gatling.kafka.actions

import com.sksamuel.avro4s
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.client.{KafkaSender, KafkaSenderImpl}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

import scala.reflect.ClassTag

class KafkaAvro4sRequestAction[K: ClassTag, V: ClassTag](
    val components: KafkaComponents,
    val attributes: Avro4sAttributes[K, V],
    val kafkaProtocol: KafkaProtocol,
    val next: Action,
    val throttler: Option[ActorRef[Throttler.Command]],
) extends KafkaBaseAction[K, V] {

  override val name: String                    = genName("kafkaAvroRequest")
  override def requestName: Expression[String] = attributes.requestName

  val statsEngine: StatsEngine                           = components.coreComponents.statsEngine
  val clock: Clock                                       = components.coreComponents.clock
  private val kafkaSender: KafkaSender[K, avro4s.Record] =
    new KafkaSenderImpl(kafkaProtocol.producerProperties, components.coreComponents)

  override def sendRequest(session: Session): Validation[Unit] = for {
    requestNameData <- attributes.requestName(session)
    producerRecord  <- avroProtocolMessage(session)
  } yield throttler match {
    case Some(th) =>
      th ! Throttler.Command.ThrottledRequest(
        session.scenario,
        () => sendAndLogAvroRecord(kafkaSender, requestNameData, producerRecord, session),
      )
    case _        => sendAndLogAvroRecord(kafkaSender, requestNameData, producerRecord, session)
  }

  private def avroProtocolMessage: Expression[ProducerRecord[K, avro4s.Record]] = s =>
    for {
      payload <- attributes.payload(s).flatMap(pl => scala.util.Try(attributes.format.to(pl)).toValidation)
      key     <- attributes.key.fold(null.asInstanceOf[K].success)(_(s))
      headers <- attributes.headers.fold(null.asInstanceOf[Headers].success)(_(s))
    } yield new ProducerRecord[K, avro4s.Record](
      kafkaProtocol.producerTopic,
      null,
      key,
      payload,
      headers,
    )

}
