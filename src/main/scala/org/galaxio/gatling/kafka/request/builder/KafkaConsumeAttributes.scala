package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

case class KafkaConsumeAttributes(
    requestName: Expression[String],
    topic: Expression[String],
    expectedMatchId: Expression[Array[Byte]],
    checks: List[KafkaCheck],
    silent: Option[Boolean],
    consumeSettingsOverride: Option[Map[String, AnyRef]],
    responseMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]],
    replyExtractions: List[KafkaReplyExtraction],
)
