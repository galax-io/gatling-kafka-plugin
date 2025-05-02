package org.galaxio.gatling.kafka.request

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers

/** Topic may either be an 'input' or an 'output' topic. If both are defined here, the serdes may need to pick one without any
  * real prior knowledge (see KafkaProtocolBuilderNew.matchByMessage)
  *
  * @param key
  *   the event Key
  * @param value
  *   the event 'data'
  * @param producerTopic
  *   The name of the Kafka topic to which the message will be sent.
  * @param consumerTopic
  *   the topic this event is for
  * @param headers
  *   any supplementary headers e.g. serde related headers
  * @param responseCode
  *   a response code
  */

final case class KafkaProtocolMessage(
    key: Array[Byte],
    value: Array[Byte],
    producerTopic: String,
    consumerTopic: String,
    headers: Option[Headers] = None,
    responseCode: Option[String] = None,
) {
  def toProducerRecord: ProducerRecord[Array[Byte], Array[Byte]] = {
    headers.fold(new ProducerRecord(producerTopic, key, value))(hs => new ProducerRecord(producerTopic, null, key, value, hs))
  }
}

object KafkaProtocolMessage {
  def from(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], inputTopic: Option[String]): KafkaProtocolMessage =
    new KafkaProtocolMessage(
      consumerRecord.key(),
      consumerRecord.value(),
      inputTopic.getOrElse("<unknown>"),
      consumerRecord.topic(),
      Option(consumerRecord.headers()),
    )
}
