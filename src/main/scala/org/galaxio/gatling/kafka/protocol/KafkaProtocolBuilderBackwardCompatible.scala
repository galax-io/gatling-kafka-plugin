package org.galaxio.gatling.kafka.protocol

import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

import scala.concurrent.duration.FiniteDuration

final case class KafkaProtocolBuilderBackwardCompatible(topic: String, props: Map[String, AnyRef], timeout: FiniteDuration) {

  def build: KafkaProtocol = {
    val serializers = Map(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )
    KafkaProtocol(topic, props ++ serializers, Map.empty, timeout, KafkaKeyMatcher)
  }

}
