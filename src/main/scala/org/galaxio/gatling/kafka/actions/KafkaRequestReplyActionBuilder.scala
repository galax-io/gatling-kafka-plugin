package org.galaxio.gatling.kafka.actions

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaMatcher, KafkaMessageMatcher}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaReplyExtraction
import org.galaxio.gatling.kafka.request.builder.KafkaRequestReplyAttributes

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

case class KafkaRequestReplyActionBuilder[K: ClassTag, V: ClassTag](attributes: KafkaRequestReplyAttributes[K, V])
    extends ActionBuilder {

  def silent: KafkaRequestReplyActionBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaRequestReplyActionBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))

  def check(checks: KafkaCheck*): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.checks).using(_ ::: checks.toList)

  def producerSettings(settings: Map[String, AnyRef]): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.producerSettingsOverride).setTo(Some(settings))

  def consumeSettings(settings: Map[String, AnyRef]): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.consumeSettingsOverride).setTo(Some(settings))

  def requestMatchBy(extractor: KafkaProtocolMessage => Array[Byte]): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.requestMatchExtractor).setTo(Some(extractor))

  def replyMatchBy(extractor: KafkaProtocolMessage => Array[Byte]): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.responseMatchExtractor).setTo(Some(extractor))

  def matchByMessage(extractor: KafkaProtocolMessage => Array[Byte]): KafkaRequestReplyActionBuilder[K, V] =
    requestMatchBy(extractor).replyMatchBy(extractor)

  def matchByKafkaMatcher(matcher: KafkaMatcher): KafkaRequestReplyActionBuilder[K, V] =
    requestMatchBy(matcher.requestMatch).replyMatchBy(matcher.responseMatch)

  def timeout(duration: FiniteDuration): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.timeout).setTo(Some(duration))

  def startTimeForTracking(startTime: Expression[Long]): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.startTimestamp).setTo(Some(startTime))

  def startTimeForTracking(sessionKey: String): KafkaRequestReplyActionBuilder[K, V] =
    startTimeForTracking(session => session(sessionKey).validate[Long])

  def saveAs(sessionKey: String)(extractor: KafkaProtocolMessage => Any): KafkaRequestReplyActionBuilder[K, V] =
    this.modify(_.attributes.replyExtractions).using(_ :+ KafkaReplyExtraction(sessionKey, extractor))

  override def build(ctx: ScenarioContext, next: Action): Action = {
    val kafkaComponents = ctx.protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)
    new KafkaRequestReplyAction[K, V](
      kafkaComponents,
      attributes,
      ctx.coreComponents.statsEngine,
      ctx.coreComponents.clock,
      next,
      ctx.coreComponents.throttler.filter(_ => ctx.throttled),
    )
  }
}
