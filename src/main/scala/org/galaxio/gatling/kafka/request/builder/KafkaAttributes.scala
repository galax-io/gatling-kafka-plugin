package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.KafkaCheck

/** @param producerTopic
  *   the topic the request is published to. Not optional: every builder that reaches an action names one, so an absent producer
  *   topic is not a state this type can represent (verdict B2). The `Option` it used to carry only ever held `Some` once the
  *   topic-less `send(...)` family went in 2.0.0.
  * @param consumerTopic
  *   the reply topic, absent for produce-only requests
  */
final case class KafkaAttributes[+K, +V](
    requestName: Expression[String],
    producerTopic: Expression[String],
    consumerTopic: Option[Expression[String]],
    key: Option[Expression[? <: K]],
    value: Expression[? <: V],
    headers: Option[Expression[Headers]],
    keySerde: Option[Serde[? <: K]],
    valueSerde: Serde[? <: V],
    checks: List[KafkaCheck],
)
