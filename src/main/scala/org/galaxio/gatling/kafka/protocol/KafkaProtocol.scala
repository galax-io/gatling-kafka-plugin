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

  private def configFingerprint(config: Map[String, AnyRef]): String =
    config.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("|")

  val kafkaProtocolKey: ProtocolKey[KafkaProtocol, KafkaComponents] = new ProtocolKey[KafkaProtocol, KafkaComponents] {
    private val senders      = new ConcurrentHashMap[String, KafkaSender]()
    private val trackerPools = new ConcurrentHashMap[String, Option[KafkaMessageTrackerPool]]()

    private def getOrCreateSender(coreComponents: CoreComponents, protocol: KafkaProtocol): KafkaSender =
      protocol.producerProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) match {
        case Some(_) =>
          val key = configFingerprint(protocol.producerProperties)
          this.senders.computeIfAbsent(
            key,
            _ => {
              val sender = KafkaSender(protocol.producerProperties)
              coreComponents.actorSystem.registerOnTermination {
                sender.close()
                senders.remove(key) // evict so a subsequent simulation doesn't get a closed sender
              }
              sender
            },
          )
        case None    =>
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
        .flatMap(_ => {
          val key = configFingerprint(protocol.consumerProperties)
          trackerPools.computeIfAbsent(
            key,
            _ => {
              // Register eviction BEFORE creating the pool. registerOnTermination is LIFO,
              // so the pool's own consumer.close() hook (registered inside its constructor)
              // will fire FIRST on shutdown, then this eviction hook fires — ensuring the
              // consumer is fully closed before a new simulation can create a replacement pool.
              coreComponents.actorSystem.registerOnTermination {
                trackerPools.remove(key)
              }
              KafkaMessageTrackerPool(
                protocol.consumerProperties,
                coreComponents.actorSystem,
                coreComponents.statsEngine,
                coreComponents.clock,
              )
            },
          )
        })

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
          getOrCreateSender(coreComponents, kafkaProtocol),
        )
  }
}

final case class KafkaProtocol(
    producerProperties: Map[String, AnyRef],
    consumerProperties: Map[String, AnyRef],
    timeout: FiniteDuration,
    messageMatcher: KafkaMatcher,
) extends Protocol
