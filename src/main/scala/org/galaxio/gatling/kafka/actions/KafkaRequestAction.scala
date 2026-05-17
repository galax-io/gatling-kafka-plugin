package org.galaxio.gatling.kafka.actions

import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session.{Expression, Session}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.header.Headers
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

object KafkaRequestAction {
  private[actions] def nextSession(session: Session, exception: Exception): Session =
    KafkaProduceActionBase.nextSession(session, exception)
}

class KafkaRequestAction[K, V](
    producer: KafkaProducer[K, V],
    components: KafkaComponents,
    attr: KafkaAttributes[K, V],
    coreComponents: CoreComponents,
    kafkaProtocol: KafkaProtocol,
    throttled: Boolean,
    next: Action,
) extends KafkaProduceActionBase[K, V, V](producer, coreComponents, kafkaProtocol, throttled, next) {

  override val name: String = genName("kafkaRequest")

  override protected def requestNameExpr: Expression[String]                  = attr.requestName
  override protected def keyExpr: Option[Expression[K]]                       = attr.key
  override protected def payloadExpr: Expression[V]                           = attr.payload
  override protected def headersExpr: Option[Expression[Headers]]             = attr.headers
  override protected def partitionExpr: Option[Expression[java.lang.Integer]] = attr.partition
  override protected def timestampExpr: Option[Expression[java.lang.Long]]    = attr.timestamp
  override protected def isSilent: Boolean                                    = attr.silent.getOrElse(false)
  override protected def transformPayload(v: V): V                            = v
}
