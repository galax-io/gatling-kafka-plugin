package org.galaxio.gatling.kafka.request

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers

/**
 * Topic may either be an 'input' or an 'output' topic.
 * If both are defined here, the serdes may need to pick one without any
 * real prior knowledge (see KafkaProtocolBuilderNew.matchByMessage)
 * @param key the event Key
 * @param value the event 'data'
 * @param topic the topic this event is for
 * @param headers any supplementary headers e.g. serde related headers
 * @param responseCode a response code
 */
case class KafkaProtocolMessage(
    key: Array[Byte],
    value: Array[Byte],
    topic: String,
    headers: Option[Headers] = None,
    responseCode: Option[String] = None,
) {
  def toProducerRecord: ProducerRecord[Array[Byte], Array[Byte]] = {
    headers.fold(new ProducerRecord(topic, key, value))(hs => new ProducerRecord(topic, null, key, value, hs))
  }
}
