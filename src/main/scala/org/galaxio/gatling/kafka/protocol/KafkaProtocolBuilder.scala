package org.galaxio.gatling.kafka.protocol

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.galaxio.gatling.kafka.protocol.KafkaProtocol._
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object KafkaProtocolBuilder {

  case class KafkaProtocolBuilderPropertiesStep(topic: String) {

    def properties(producerSettings: Map[String, Object]): KafkaProtocolBuilderBackwardCompatible =
      KafkaProtocolBuilderBackwardCompatible(topic, producerSettings, 60.seconds)
  }

  @deprecated("use topic definition in kafka request builders", "1.0.0")
  def topic(name: String): KafkaProtocolBuilderPropertiesStep =
    KafkaProtocolBuilderPropertiesStep(name)

  @deprecated(
    "separate definition of the protocol for the requestReply scheme is no longer required; use producerSettings right away",
    "1.0.0",
  )
  def requestReply: KafkaProtocolBuilder.type = KafkaProtocolBuilder

  def producerSettings(ps: Map[String, AnyRef]): KPProducerSettingsStep = KPProducerSettingsStep(ps)

  def producerSettings(pp: (String, AnyRef), pps: (String, AnyRef)*): KPProducerSettingsStep = producerSettings(
    (pp +: pps).toMap,
  )

  def properties(producerSettings: Map[String, Object]): KafkaProtocolBuilder =
    KafkaProtocolBuilder(producerSettings, Map.empty, 60.seconds)

  def properties(p: (String, AnyRef), ps: (String, AnyRef)*): KafkaProtocolBuilder =
    properties((p +: ps).toMap)

  case class KPProducerSettingsStep(producerSettings: Map[String, AnyRef]) {
    def consumeSettings(cs: Map[String, AnyRef]): KPConsumeSettingsStep = KPConsumeSettingsStep(producerSettings, cs)

    def consumeSettings(cp: (String, AnyRef), cps: (String, AnyRef)*): KPConsumeSettingsStep = consumeSettings(
      (cp +: cps).toMap,
    )

    def timeout(t: FiniteDuration): KafkaProtocolBuilder = KafkaProtocolBuilder(producerSettings, Map.empty, t)
    def withDefaultTimeout: KafkaProtocolBuilder         = KafkaProtocolBuilder(producerSettings, Map.empty, 60.seconds)

  }

  case class KPConsumeSettingsStep(producerSettings: Map[String, AnyRef], consumeSettings: Map[String, AnyRef]) {
    def timeout(t: FiniteDuration): KafkaProtocolBuilder = KafkaProtocolBuilder(producerSettings, consumeSettings, t)
    def withDefaultTimeout: KafkaProtocolBuilder         = KafkaProtocolBuilder(producerSettings, consumeSettings, 60.seconds)
  }
}

final case class KafkaProtocolBuilder(
    producerSettings: Map[String, AnyRef],
    consumeSettings: Map[String, AnyRef],
    timeout: FiniteDuration,
    messageMatcher: KafkaMatcher = KafkaKeyMatcher,
) {

  def matchByValue: KafkaProtocolBuilder =
    messageMatcher(KafkaValueMatcher)

  def matchByMessage(keyExtractor: KafkaProtocolMessage => Array[Byte]): KafkaProtocolBuilder =
    messageMatcher(KafkaMessageMatcher(keyExtractor))

  private def messageMatcher(matcher: KafkaMatcher): KafkaProtocolBuilder =
    copy(messageMatcher = matcher)

  def build: KafkaProtocol = {

    val serializers = Map(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )

    val consumeDefaults = Map(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> Serdes.ByteArray().deserializer().getClass.getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> Serdes.ByteArray().deserializer().getClass.getName,
    )

    KafkaProtocol("kafka-test", producerSettings ++ serializers, consumeDefaults ++ consumeSettings, timeout, messageMatcher)
  }
}
