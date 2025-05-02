package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.reflect.ClassTag

final class KafkaRequestActionBuilder[+K: ClassTag, +V: ClassTag](attributes: KafkaAttributes[K, V]) extends ActionBuilder {

  override def build(ctx: ScenarioContext, next: Action): Action = {

    val kafkaComponents =
      ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    new KafkaRequestAction(
      kafkaComponents,
      attributes,
      ctx.coreComponents,
      next,
      ctx.coreComponents.throttler.filter(_ => ctx.throttled),
    )
  }
}
