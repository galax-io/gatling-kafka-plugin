package org.galaxio.gatling.kafka.request.builder

import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

case class KafkaReplyExtraction(
    sessionKey: String,
    extractor: KafkaProtocolMessage => Any,
)
