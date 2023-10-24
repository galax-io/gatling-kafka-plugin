package io.cosmospf.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer
import io.cosmospf.gatling.kafka.KafkaCheck

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
)
