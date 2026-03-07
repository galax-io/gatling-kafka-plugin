package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, RecordMetadata}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

trait KafkaSender {
  val executionContext: ExecutionContext
  def send(protocolMessage: KafkaProtocolMessage)(
      onSuccess: RecordMetadata => Unit,
      onFailure: Throwable => Unit,
  ): Unit
  def close(): Unit
}

object KafkaSender {
  private final class Impl(producer: Producer[Array[Byte], Array[Byte]])(implicit ec: ExecutionContext) extends KafkaSender {
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

    override val executionContext: ExecutionContext = ec
  }

  def apply(producerSettings: Map[String, AnyRef])(implicit ec: ExecutionContext): KafkaSender = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerSettings.asJava)
    fromProducer(producer)
  }

  private[client] def fromProducer(
      producer: Producer[Array[Byte], Array[Byte]],
  )(implicit ec: ExecutionContext): KafkaSender =
    new Impl(producer)
}
