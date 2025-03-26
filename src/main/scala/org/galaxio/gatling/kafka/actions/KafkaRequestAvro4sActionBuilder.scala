package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

import scala.jdk.CollectionConverters._

class KafkaRequestAvro4sActionBuilder[K, V](attr: Avro4sAttributes[K, V]) extends ActionBuilder with NameGen {
  override def build(ctx: ScenarioContext, next: Action): Action = {

    val kafkaComponents: KafkaProtocol.Components = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    val producer = new KafkaProducer[K, GenericRecord](kafkaComponents.kafkaProtocol.producerProperties.asJava)

    ctx.coreComponents.actorSystem.registerOnTermination(producer.close())

    new KafkaAvro4sRequestAction(
      producer,
      kafkaComponents,
      attr,
      ctx.coreComponents,
      kafkaComponents.kafkaProtocol,
      next,
      ctx.coreComponents.throttler.filter(_ => ctx.throttled),
    )
  }
}
