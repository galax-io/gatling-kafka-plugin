package org.galaxio.gatling.kafka.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolKey}
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
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

//    private val blockingPool                                              = Executors.newCachedThreadPool()
//    private val ec: ExecutionContextExecutorService                       = ExecutionContext.fromExecutorService(blockingPool)
    private val senderRef: AtomicReference[KafkaSender]                   = new AtomicReference[KafkaSender]()
    private val trackersPoolRef: AtomicReference[KafkaMessageTrackerPool] = new AtomicReference[KafkaMessageTrackerPool]()

    private def getOrCreateSender(protocol: KafkaProtocol): KafkaSender = {
      do {
        val sender = senderRef.get()
        if (sender != null) {
          return sender
        }
      } while (!senderRef.compareAndSet(null, KafkaSender(protocol.producerProperties)))
      senderRef.get()
    }

    private def getOrCreateTrackerPool(coreComponents: CoreComponents, protocol: KafkaProtocol): KafkaMessageTrackerPool = {
      do {
        val pool = trackersPoolRef.get()
        if (pool != null) {
          return pool
        }
      } while (
        !trackersPoolRef.compareAndSet(
          null,
          KafkaMessageTrackerPool(
            protocol.consumerProperties,
            coreComponents.actorSystem,
            coreComponents.statsEngine,
            coreComponents.clock,
          ),
        )
      )
      trackersPoolRef.get()
    }

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
//    @deprecated("use topics in requests builders", "1.0.0")
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
