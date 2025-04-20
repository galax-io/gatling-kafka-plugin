package org.galaxio.gatling.kafka.actions

import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.internal.quicklens._
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.KafkaRequestReplyAttributes

import scala.reflect.ClassTag

case class KafkaRequestReplyActionBuilder[K: ClassTag, V: ClassTag](attributes: KafkaRequestReplyAttributes[K, V])
    extends ActionBuilder {
  def check(checks: KafkaCheck*): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.checks).using(_ ::: checks.toList)

  override def build(ctx: ScenarioContext, next: Action): Action = {
    val kafkaComponents = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)
    new KafkaRequestReplyAction[K, V](
      kafkaComponents,
      attributes,
      ctx.coreComponents.statsEngine,
      ctx.coreComponents.clock,
      next,
      ctx.coreComponents.throttler,
    )
  }
}
