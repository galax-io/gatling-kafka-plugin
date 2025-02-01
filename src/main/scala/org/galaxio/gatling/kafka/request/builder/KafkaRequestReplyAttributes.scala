package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.KafkaCheck

case class KafkaRequestReplyAttributes[K, V](
    requestName: Expression[String],
    inputTopic: Expression[String],
    outputTopic: Expression[String],
    key: Expression[K],
    value: Expression[V],
    headers: Either[Expression[String], Headers],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    checks: List[KafkaCheck],
)
