package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

import scala.reflect.ClassTag

class KafkaRequestAvro4sActionBuilder[K: ClassTag, V: ClassTag](attr: Avro4sAttributes[K, V])
    extends ActionBuilder with NameGen {
  override def build(ctx: ScenarioContext, next: Action): Action = {

    val kafkaComponents = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    new KafkaAvro4sRequestAction(
      kafkaComponents,
      attr,
      kafkaComponents.kafkaProtocol,
      next,
      ctx.coreComponents.throttler.filter(_ => ctx.throttled),
    )
  }
}
