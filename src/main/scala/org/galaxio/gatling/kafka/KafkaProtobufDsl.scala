package org.galaxio.gatling.kafka

import io.gatling.core.check.CheckBuilder
import org.galaxio.gatling.kafka.checks.KafkaCheckMaterializer.KafkaMessageCheckType
import org.galaxio.gatling.kafka.checks.ProtobufBodyCheckBuilder
import org.galaxio.gatling.kafka.request.{KafkaProtobufSerdes, KafkaProtocolMessage}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

trait KafkaProtobufDsl extends KafkaProtobufSerdes {

  def protobufBody[T <: GeneratedMessage](implicit
      companion: GeneratedMessageCompanion[T],
  ): CheckBuilder.Find[KafkaMessageCheckType, KafkaProtocolMessage, T] =
    ProtobufBodyCheckBuilder._protobufBody[T]

}

object KafkaProtobufDsl extends KafkaProtobufDsl
