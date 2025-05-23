package org.galaxio.gatling.kafka.checks

import io.gatling.commons.validation.{TryWrapper, Validation}
import io.gatling.core.check.CheckBuilder.Find
import io.gatling.core.check.{CheckBuilder, CheckMaterializer, Extractor}
import io.gatling.core.session.ExpressionSuccessWrapper
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.checks.KafkaCheckMaterializer.KafkaMessageCheckType
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.util.Try

object AvroBodyCheckBuilder {
  private type KafkaCheckMaterializer[T, P] = CheckMaterializer[T, KafkaCheck, KafkaProtocolMessage, P]

  def _avroBody[T: Serde]: CheckBuilder.Find[KafkaMessageCheckType, KafkaProtocolMessage, T] = {
    val tExtractor = new Extractor[KafkaProtocolMessage, T] {
      val name                                                         = "avroBody"
      val arity                                                        = "find"
      def apply(prepared: KafkaProtocolMessage): Validation[Option[T]] = {
        Try(Option(implicitly[Serde[T]].deserializer().deserialize(prepared.consumerTopic, prepared.value))).toValidation
      }
    }.expressionSuccess

    new Find.Default[KafkaMessageCheckType, KafkaProtocolMessage, T](tExtractor, displayActualValue = true)
  }
}
