package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, RecordMetadata}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.jdk.CollectionConverters._

trait KafkaSender {
  def send(protocolMessage: KafkaProtocolMessage)(
      onSuccess: RecordMetadata => Unit,
      onFailure: Throwable => Unit,
  ): Unit
  def close(): Unit
}

object KafkaSender {
  private final class Impl(producer: Producer[Array[Byte], Array[Byte]]) extends KafkaSender {
    override def send(
        protocolMessage: KafkaProtocolMessage,
    )(onSuccess: RecordMetadata => Unit, onFailure: Throwable => Unit): Unit = {
      producer.send(
        protocolMessage.toProducerRecord,
        (metadata: RecordMetadata, exception: Throwable) =>
          if (exception == null)
            onSuccess(metadata)
          else
            onFailure(exception),
      )

    }

    override def close(): Unit =
      producer.close()

  }

  def apply(producerSettings: Map[String, AnyRef]): KafkaSender = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerSettings.asJava)
    new Impl(producer)
  }
}
