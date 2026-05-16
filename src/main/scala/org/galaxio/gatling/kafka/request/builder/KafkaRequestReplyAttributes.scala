package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher

case class KafkaRequestReplyAttributes[K, V](
    requestName: Expression[String],
    inputTopic: Expression[String],
    outputTopic: Expression[String],
    key: Expression[K],
    value: Expression[V],
    headers: Option[Expression[Headers]],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    checks: List[KafkaCheck],
    silent: Option[Boolean],
    producerSettingsOverride: Option[Map[String, AnyRef]],
    consumeSettingsOverride: Option[Map[String, AnyRef]],
    requestMatchExtractor: Option[org.galaxio.gatling.kafka.request.KafkaProtocolMessage => Array[Byte]],
    responseMatchExtractor: Option[org.galaxio.gatling.kafka.request.KafkaProtocolMessage => Array[Byte]],
    replyExtractions: List[KafkaReplyExtraction],
)
