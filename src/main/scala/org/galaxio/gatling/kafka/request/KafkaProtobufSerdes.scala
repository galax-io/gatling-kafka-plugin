package org.galaxio.gatling.kafka.request

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

trait KafkaProtobufSerdes {

  implicit def scalapbSerde[T <: GeneratedMessage](implicit
      companion: GeneratedMessageCompanion[T],
  ): Serde[T] = new Serde[T] {
    override def serializer(): Serializer[T] =
      (_: String, data: T) => if (data == null) null else data.toByteArray

    override def deserializer(): Deserializer[T] =
      (_: String, bytes: Array[Byte]) => if (bytes == null) null.asInstanceOf[T] else companion.parseFrom(bytes)
  }

}

object KafkaProtobufSerdes extends KafkaProtobufSerdes
