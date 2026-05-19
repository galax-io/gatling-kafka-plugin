package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.KafkaCheck

final case class KafkaAttributes[+K, +V](
    requestName: Expression[String],
    producerTopic: Option[Expression[String]],
    consumerTopic: Option[Expression[String]],
    key: Option[Expression[? <: K]],
    value: Expression[? <: V],
    headers: Option[Expression[Headers]],
    keySerde: Option[Serde[? <: K]],
    valueSerde: Serde[? <: V],
    checks: List[KafkaCheck],
)
