package org.galaxio.gatling.kafka.actions

import com.softwaremill.quicklens._
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.{KafkaRequestReplyAttributes, RequestBuilder}

import scala.reflect.ClassTag

case class KafkaRequestReplyActionBuilder[K: ClassTag, V: ClassTag](attributes: KafkaRequestReplyAttributes[K, V])
    extends ActionBuilder with NameGen {

  def silent: KafkaRequestReplyActionBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaRequestReplyActionBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))

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
