package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

object KafkaConsumeAttributes {
  val ConsumeAnyMatchId: Array[Byte] = "__consume_any__".getBytes(java.nio.charset.StandardCharsets.UTF_8)
}

case class KafkaConsumeAttributes(
    requestName: Expression[String],
    topic: Expression[String],
    expectedMatchId: Option[Expression[Array[Byte]]],
    checks: List[KafkaCheck],
    silent: Option[Boolean],
    consumeSettingsOverride: Option[Map[String, AnyRef]],
    responseMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]],
    replyExtractions: List[KafkaReplyExtraction],
)
