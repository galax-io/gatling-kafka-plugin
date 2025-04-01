package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

class KafkaRequestActionBuilder[K, V](attr: KafkaAttributes[K, V]) extends ActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action = {

    val kafkaComponents = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    new KafkaRequestAction(
      kafkaComponents,
      attr,
      kafkaComponents.kafkaProtocol,
      next,
      ctx.coreComponents.throttler.filter(_ => ctx.throttled),
    )
  }
}
