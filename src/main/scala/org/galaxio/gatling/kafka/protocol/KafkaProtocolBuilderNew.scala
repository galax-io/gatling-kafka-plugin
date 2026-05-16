package org.galaxio.gatling.kafka.protocol

import io.gatling.core.session.Expression
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.galaxio.gatling.kafka.protocol.KafkaProtocol._
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object KafkaProtocolBuilderNew {
  def producerSettings(ps: Map[String, AnyRef]): KPProducerSettingsStep = KPProducerSettingsStep(ps)

  case class KPProducerSettingsStep(producerSettings: Map[String, AnyRef]) {
    def consumeSettings(cs: Map[String, AnyRef]): KPConsumeSettingsStep = KPConsumeSettingsStep(producerSettings, cs)
  }

  case class KPConsumeSettingsStep(producerSettings: Map[String, AnyRef], consumeSettings: Map[String, AnyRef]) {
    def timeout(t: FiniteDuration): KafkaProtocolBuilderNew = KafkaProtocolBuilderNew(producerSettings, consumeSettings, t)
    def withDefaultTimeout: KafkaProtocolBuilderNew         = KafkaProtocolBuilderNew(producerSettings, consumeSettings, 60.seconds)
  }
}

case class KafkaProtocolBuilderNew(
    producerSettings: Map[String, AnyRef],
    consumeSettings: Map[String, AnyRef],
    timeout: FiniteDuration,
    messageMatcher: KafkaMatcher = KafkaKeyMatcher,
) extends {

  def matchByKafkaMatcher(kafkaMatcher: KafkaMatcher): KafkaProtocolBuilderNew =
    messageMatcher(kafkaMatcher)

  def matchByValue: KafkaProtocolBuilderNew =
    messageMatcher(KafkaValueMatcher)

  def matchByMessage(keyExtractor: KafkaProtocolMessage => Array[Byte]): KafkaProtocolBuilderNew =
    messageMatcher(KafkaMessageMatcher(keyExtractor))

  private def messageMatcher(matcher: KafkaMatcher): KafkaProtocolBuilderNew =
    copy(messageMatcher = matcher)

  def build: KafkaProtocol = {

    val serializers = Map(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )

    val consumeDefaults = Map(
      ConsumerConfig.GROUP_ID_CONFIG              -> s"gatling-test-${java.util.UUID.randomUUID()}",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    )

    KafkaProtocol("test", producerSettings ++ serializers, consumeDefaults ++ consumeSettings, timeout, messageMatcher)
  }
}
