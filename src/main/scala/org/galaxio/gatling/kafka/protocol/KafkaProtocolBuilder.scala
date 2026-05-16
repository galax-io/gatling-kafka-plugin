package org.galaxio.gatling.kafka.protocol

import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case object KafkaProtocolBuilder {

  def topic(name: String): KafkaProtocolBuilderPropertiesStep =
    KafkaProtocolBuilderPropertiesStep(name, Map.empty[String, Object])

  def requestReply: KafkaProtocolBuilderNew.type = KafkaProtocolBuilderNew

}

case class KafkaProtocolBuilder(topic: String, props: Map[String, Object], timeout: FiniteDuration = 60.seconds) {

  def timeout(t: FiniteDuration): KafkaProtocolBuilder = copy(timeout = t)

  def build = new KafkaProtocol(topic, props, props, timeout, KafkaKeyMatcher)

}
