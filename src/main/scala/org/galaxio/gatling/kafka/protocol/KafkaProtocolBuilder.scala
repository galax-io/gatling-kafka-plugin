package org.galaxio.gatling.kafka.protocol

import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

import scala.concurrent.duration.DurationInt

case object KafkaProtocolBuilder {

  def topic(name: String): KafkaProtocolBuilderPropertiesStep =
    KafkaProtocolBuilderPropertiesStep(name, Map.empty[String, Object])

  def requestReply: KafkaProtocolBuilderNew.type = KafkaProtocolBuilderNew

}

case class KafkaProtocolBuilder(topic: String, props: Map[String, Object]) {

  def build = new KafkaProtocol(topic, props, props, 60.seconds, KafkaKeyMatcher)

}
