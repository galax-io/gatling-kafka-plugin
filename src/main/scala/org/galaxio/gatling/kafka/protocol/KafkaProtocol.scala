package org.galaxio.gatling.kafka.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolKey}
import org.galaxio.gatling.kafka.client.{KafkaSender, KafkaMessageTrackerPool}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.Executors
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object KafkaProtocol {

  trait KafkaMatcher {
    def requestMatch(msg: KafkaProtocolMessage): Array[Byte]
    def responseMatch(msg: KafkaProtocolMessage): Array[Byte]
  }

  object KafkaKeyMatcher extends KafkaMatcher {
    override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
    override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.key
  }

  object KafkaValueMatcher extends KafkaMatcher {
    override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.value
    override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
  }

  case class KafkaMessageMatcher(keyExtractor: KafkaProtocolMessage => Array[Byte]) extends KafkaMatcher {
    override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = keyExtractor(msg)
    override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = keyExtractor(msg)
  }

  type Components = KafkaComponents

  val kafkaProtocolKey: ProtocolKey[KafkaProtocol, Components] = new ProtocolKey[KafkaProtocol, Components] {
    override def protocolClass: Class[Protocol] =
      classOf[KafkaProtocol].asInstanceOf[Class[Protocol]]

    override def defaultProtocolValue(configuration: GatlingConfiguration): KafkaProtocol =
      throw new IllegalStateException("Can't provide a default value for KafkaProtocol")

    override def newComponents(coreComponents: CoreComponents): KafkaProtocol => KafkaComponents =
      kafkaProtocol => {
        val blockingPool                                 = Executors.newCachedThreadPool()
        implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(blockingPool)

        val sender       = KafkaSender(kafkaProtocol.producerProperties)
        val trackersPool = new KafkaMessageTrackerPool(
          kafkaProtocol.consumerProperties,
          coreComponents.actorSystem,
          coreComponents.statsEngine,
          coreComponents.clock,
        )
        coreComponents.actorSystem.registerOnTermination {
          sender.close()
          ec.shutdown()
          blockingPool.shutdown()
        }
        KafkaComponents(coreComponents, kafkaProtocol, trackersPool, sender)
      }
  }
}

case class KafkaProtocol(
    producerTopic: String,
    producerProperties: Map[String, AnyRef],
    consumerProperties: Map[String, AnyRef],
    timeout: FiniteDuration,
    messageMatcher: KafkaMatcher,
) extends Protocol {

  def topic(t: String): KafkaProtocol = copy(producerTopic = t)

  def properties(properties: Map[String, AnyRef]): KafkaProtocol =
    copy(producerProperties = properties)

  def producerProperties(properties: Map[String, AnyRef]): KafkaProtocol = copy(producerProperties = properties)
  def consumerProperties(properties: Map[String, AnyRef]): KafkaProtocol = copy(consumerProperties = properties)
  def timeout(t: FiniteDuration): KafkaProtocol                          = copy(timeout = t)
}
