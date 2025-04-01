package org.galaxio.gatling.kafka.client

import io.gatling.core.CoreComponents
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.galaxio.gatling.kafka.KafkaLogging

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

trait KafkaSender[K, V] {

  val producer: KafkaProducer[K, V]

  def send(
      protocolMessage: ProducerRecord[K, V],
      onSuccess: RecordMetadata => Unit,
      onFailure: Throwable => Unit,
  ): Unit

  def close(): Unit
}

/** For sending Records
  * @param producerSettings
  *   Kafka Producer settings
  */
class KafkaSenderImpl[K, V](producerSettings: Map[String, AnyRef], coreComponents: CoreComponents)
    extends KafkaSender[K, V] with KafkaLogging {

  private val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  override val producer                           = new KafkaProducer[K, V](producerSettings.asJava)
  coreComponents.actorSystem.registerOnTermination(close())

  override def send(
      protocolMessage: ProducerRecord[K, V],
      onSuccess: RecordMetadata => Unit,
      onFailure: Throwable => Unit,
  ): Unit = {
    Future(producer.send(protocolMessage).get())(ec).onComplete {
      case Success(value)     => onSuccess(value)
      case Failure(exception) => onFailure(exception)
    }(ec)
  }

  override def close(): Unit = {
    logger.debug("Closing KafkaSender and Execution Context")
    producer.close()
    ec.shutdown()
  }
}
