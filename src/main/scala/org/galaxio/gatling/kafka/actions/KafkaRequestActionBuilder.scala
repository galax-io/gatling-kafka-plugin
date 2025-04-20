package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.jdk.CollectionConverters._

class KafkaRequestActionBuilder[K, V](attr: KafkaAttributes[K, V]) extends ActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action = {

    import ctx._

    val kafkaComponents =
      protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    val producer = new KafkaProducer[K, V](kafkaComponents.kafkaProtocol.producerProperties.asJava)

    coreComponents.actorSystem.registerOnTermination(producer.close())

    new KafkaRequestAction(
      producer,
      kafkaComponents,
      attr,
      coreComponents,
      kafkaComponents.kafkaProtocol,
      throttled,
      next,
    )
  }
}
