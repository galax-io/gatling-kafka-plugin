package org.galaxio.gatling.kafka.checks

import io.gatling.commons.validation._
import io.gatling.core.check.CheckBuilder.Find
import io.gatling.core.check.{CheckBuilder, Extractor}
import io.gatling.core.session.ExpressionSuccessWrapper
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.checks.KafkaCheckMaterializer.KafkaMessageCheckType
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.util.Try

object ProtobufBodyCheckBuilder {
  private type KafkaCheckMat[T, P] = io.gatling.core.check.CheckMaterializer[T, KafkaCheck, KafkaProtocolMessage, P]

  def _protobufBody[T <: GeneratedMessage](implicit
      companion: GeneratedMessageCompanion[T],
  ): CheckBuilder.Find[KafkaMessageCheckType, KafkaProtocolMessage, T] = {
    val tExtractor = new Extractor[KafkaProtocolMessage, T] {
      val name                                                         = "protobufBody"
      val arity                                                        = "find"
      def apply(prepared: KafkaProtocolMessage): Validation[Option[T]] =
        Try(Option(companion.parseFrom(prepared.value))).toValidation
    }.expressionSuccess

    new Find.Default[KafkaMessageCheckType, KafkaProtocolMessage, T](tExtractor, displayActualValue = true)
  }
}
