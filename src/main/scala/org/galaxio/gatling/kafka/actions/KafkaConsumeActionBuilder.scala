package org.galaxio.gatling.kafka.actions

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaMatcher, KafkaMessageMatcher}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.{KafkaConsumeAttributes, KafkaReplyExtraction}

object KafkaConsumeActionBuilder {
  def create(
      requestName: io.gatling.core.session.Expression[String],
      topic: io.gatling.core.session.Expression[String],
      expectedMatchId: io.gatling.core.session.Expression[Array[Byte]],
  ): KafkaConsumeActionBuilder =
    KafkaConsumeActionBuilder(
      KafkaConsumeAttributes(
        requestName = requestName,
        topic = topic,
        expectedMatchId = expectedMatchId,
        checks = List.empty,
        silent = None,
        consumeSettingsOverride = None,
        responseMatchExtractor = None,
        replyExtractions = List.empty,
      ),
    )
}

case class KafkaConsumeActionBuilder(attributes: KafkaConsumeAttributes) extends ActionBuilder {

  def silent: KafkaConsumeActionBuilder = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaConsumeActionBuilder = this.modify(_.attributes.silent).setTo(Some(false))

  def check(checks: KafkaCheck*): KafkaConsumeActionBuilder =
    this.modify(_.attributes.checks).using(_ ::: checks.toList)

  def consumeSettings(settings: Map[String, AnyRef]): KafkaConsumeActionBuilder =
    this.modify(_.attributes.consumeSettingsOverride).setTo(Some(settings))

  def replyMatchBy(extractor: KafkaProtocolMessage => Array[Byte]): KafkaConsumeActionBuilder =
    this.modify(_.attributes.responseMatchExtractor).setTo(Some(extractor))

  def matchByMessage(extractor: KafkaProtocolMessage => Array[Byte]): KafkaConsumeActionBuilder =
    replyMatchBy(extractor)

  def matchByKafkaMatcher(matcher: KafkaMatcher): KafkaConsumeActionBuilder =
    replyMatchBy(matcher.responseMatch)

  def saveAs(sessionKey: String)(extractor: KafkaProtocolMessage => Any): KafkaConsumeActionBuilder =
    this.modify(_.attributes.replyExtractions).using(_ :+ KafkaReplyExtraction(sessionKey, extractor))

  override def build(ctx: ScenarioContext, next: Action): Action = {
    val kafkaComponents = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)
    new KafkaConsumeAction(
      kafkaComponents,
      attributes,
      ctx.coreComponents.statsEngine,
      ctx.coreComponents.clock,
      next,
      ctx.coreComponents.throttler,
    )
  }
}
