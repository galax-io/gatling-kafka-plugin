package org.galaxio.gatling.kafka.actions

import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

class KafkaAvro4sRequestAction[K, V](
    producer: KafkaProducer[K, GenericRecord],
    components: KafkaComponents,
    attr: Avro4sAttributes[K, V],
    coreComponents: CoreComponents,
    kafkaProtocol: KafkaProtocol,
    throttled: Boolean,
    next: Action,
) extends KafkaProduceActionBase[K, V, GenericRecord](producer, coreComponents, kafkaProtocol, throttled, next) {

  override val name: String = genName("kafkaAvroRequest")

  override protected def requestNameExpr: Expression[String]      = attr.requestName
  override protected def keyExpr: Option[Expression[K]]           = attr.key
  override protected def payloadExpr: Expression[V]               = attr.payload
  override protected def headersExpr: Option[Expression[Headers]] = attr.headers
  override protected def isSilent: Boolean                        = attr.silent.getOrElse(false)
  override protected def transformPayload(v: V): GenericRecord    = attr.format.to(v)
}
