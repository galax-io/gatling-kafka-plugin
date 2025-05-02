package org.galaxio.gatling.kafka

import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session._
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.header.{Header, Headers}
import org.galaxio.gatling.kafka.checks.KafkaCheckSupport
import org.galaxio.gatling.kafka.protocol.{KafkaProtocol, KafkaProtocolBuilder, KafkaProtocolBuilderBackwardCompatible}
import org.galaxio.gatling.kafka.request.KafkaSerdesImplicits
import org.galaxio.gatling.kafka.request.builder.{KafkaRequestBuilderBase, RequestBuilder}

import scala.jdk.CollectionConverters._

trait KafkaDsl extends KafkaCheckSupport with KafkaSerdesImplicits {

  val kafka: KafkaProtocolBuilder.type = KafkaProtocolBuilder

  def kafka(requestName: Expression[String]): KafkaRequestBuilderBase =
    KafkaRequestBuilderBase(requestName)

  implicit def kafkaProtocolBuilder2kafkaProtocol(builder: KafkaProtocolBuilder): KafkaProtocol = builder.build

  implicit def kafkaProtocolBuilderBackwardCompatible2kafkaProtocol(
      builder: KafkaProtocolBuilderBackwardCompatible,
  ): KafkaProtocol = builder.build

  implicit def kafkaRequestBuilder2ActionBuilder[K, V](builder: RequestBuilder[K, V]): ActionBuilder = builder.build

  implicit def listHeaderToHeaders(lh: Expression[List[Header]]): Expression[Headers] = lh.map(l => new RecordHeaders(l.asJava))

  implicit def listHeaderToExpression(lh: List[Header]): Expression[Headers] = listHeaderToHeaders(lh.expressionSuccess)

}
