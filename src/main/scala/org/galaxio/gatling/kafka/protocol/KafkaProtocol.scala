package org.galaxio.gatling.kafka.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolKey}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

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

  val kafkaProtocolKey: ProtocolKey[KafkaProtocol, KafkaComponents] = new ProtocolKey[KafkaProtocol, KafkaComponents] {
    private val senders      = new ConcurrentHashMap[String, KafkaSender]()
    private val trackerPools = new ConcurrentHashMap[String, Option[KafkaMessageTrackerPool]]()

    private def getOrCreateSender(protocol: KafkaProtocol): KafkaSender =
      protocol.producerProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) match {
        case Some(servers) => this.senders.computeIfAbsent(servers.toString, _ => KafkaSender(protocol.producerProperties))
        case None          =>
          throw new IllegalArgumentException(
            s"Producer settings don't set the required '${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}' parameter",
          )
      }

    private def getOrCreateTrackerPool(
        coreComponents: CoreComponents,
        protocol: KafkaProtocol,
    ): Option[KafkaMessageTrackerPool] =
      protocol.consumerProperties
        .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
        .flatMap(servers =>
          trackerPools.computeIfAbsent(
            servers.toString,
            _ =>
              KafkaMessageTrackerPool(
                protocol.consumerProperties,
                coreComponents.actorSystem,
                coreComponents.statsEngine,
                coreComponents.clock,
              ),
          ),
        )

    override def protocolClass: Class[Protocol] =
      classOf[KafkaProtocol].asInstanceOf[Class[Protocol]]

    override def defaultProtocolValue(configuration: GatlingConfiguration): KafkaProtocol =
      throw new IllegalStateException("Can't provide a default value for KafkaProtocol")

    override def newComponents(coreComponents: CoreComponents): KafkaProtocol => KafkaComponents =
      kafkaProtocol =>
        KafkaComponents(
          coreComponents,
          kafkaProtocol,
          getOrCreateTrackerPool(coreComponents, kafkaProtocol),
          getOrCreateSender(kafkaProtocol),
        )
  }
}

final case class KafkaProtocol(
    producerTopic: String, // TODO: remove after 1.1.0 (when topic moved from protocol to request builders)
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
