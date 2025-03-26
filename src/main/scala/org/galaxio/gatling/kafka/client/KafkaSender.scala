package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, RecordMetadata}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

trait KafkaSender {
  def executionContext(): ExecutionContext
  implicit def send(protocolMessage: KafkaProtocolMessage)(onSuccess: RecordMetadata => Unit, onFailure: Throwable => Unit)(
      implicit ec: ExecutionContext,
  ): Unit
  def close(): Unit
}

class KafkaSenderImpl(producerSettings: Map[String, AnyRef]) extends KafkaSender {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private val producer                             = new KafkaProducer[Array[Byte], Array[Byte]](producerSettings.asJava)

  override def close(): Unit = {
    producer.close()
    ec.shutdown()
  }

  override implicit def send(
      protocolMessage: KafkaProtocolMessage,
  )(onSuccess: RecordMetadata => Unit, onFailure: Throwable => Unit)(implicit ec: ExecutionContext): Unit = {
    Future(producer.send(protocolMessage.toProducerRecord).get())(ec).onComplete {
      case Success(value)     => onSuccess(value)
      case Failure(exception) => onFailure(exception)
    }(ec)
  }

  override def executionContext(): ExecutionContextExecutorService = ec
}
