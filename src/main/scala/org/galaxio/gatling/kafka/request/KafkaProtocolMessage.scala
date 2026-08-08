package org.galaxio.gatling.kafka.request

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers

/** Topic may either be an 'input' or an 'output' topic. If both are defined here, the serdes may need to pick one without any
  * real prior knowledge (see KafkaProtocolBuilderNew.matchByMessage)
  *
  * `key` and `value` are both nullable, and an absent one is distinct from an empty one — the distinction Kafka itself draws. A
  * null key means the record carries none, which is what makes the partitioner spread it round-robin; a null value is a
  * tombstone. `from` has always copied both straight off the `ConsumerRecord`, so a received message can carry either. Code
  * reading them must handle null rather than assume a zero-length array (issues #167 and #168).
  *
  * @param key
  *   the event Key, or null when the record has none
  * @param value
  *   the event 'data', or null for a tombstone
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
    KafkaProtocolMessage(
      consumerRecord.key(),
      consumerRecord.value(),
      inputTopic.getOrElse(consumerRecord.topic()),
      consumerRecord.topic(),
      Option(consumerRecord.headers()),
    )
}
