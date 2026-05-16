package org.galaxio.gatling.kafka.actions

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.commons.validation.Success
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.KafkaCheck
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.{KafkaConsumeAttributes, KafkaReplyExtraction, KafkaSerializedExpression}

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

object KafkaConsumeActionBuilder {
  def create(
      requestName: io.gatling.core.session.Expression[String],
      topic: io.gatling.core.session.Expression[String],
  ): KafkaConsumeActionBuilder =
    KafkaConsumeActionBuilder(
      KafkaConsumeAttributes(
        requestName = requestName,
        topic = topic,
        expectedMatchId = None,
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

  def matchIdForTracking(matchId: Expression[Array[Byte]]): KafkaConsumeActionBuilder =
    this.modify(_.attributes.expectedMatchId).setTo(Some(matchId))

  def matchIdForTracking(matchId: Array[Byte]): KafkaConsumeActionBuilder =
    matchIdForTracking(_ => Success(matchId))

  def keyForTracking[K: Serde: ClassTag](key: Expression[K]): KafkaConsumeActionBuilder =
    matchIdForTracking(KafkaSerializedExpression(attributes.topic, key)).replyMatchBy(_.key)

  def keyForTracking[K: Serde: ClassTag](key: K): KafkaConsumeActionBuilder =
    matchIdForTracking(KafkaSerializedExpression.static(attributes.topic, key)).replyMatchBy(_.key)

  def payloadForTracking[V: Serde: ClassTag](payload: Expression[V]): KafkaConsumeActionBuilder =
    matchIdForTracking(KafkaSerializedExpression(attributes.topic, payload)).replyMatchBy(_.value)

  def payloadForTracking[V: Serde: ClassTag](payload: V): KafkaConsumeActionBuilder =
    matchIdForTracking(KafkaSerializedExpression.static(attributes.topic, payload)).replyMatchBy(_.value)

  def headerForTracking(headerName: String, headerValue: Expression[String]): KafkaConsumeActionBuilder =
    matchIdForTracking(headerValue.map(_.getBytes(StandardCharsets.UTF_8))).replyMatchBy { message =>
      message.headers
        .flatMap(headers => Option(headers.lastHeader(headerName)))
        .map(_.value())
        .orNull
    }

  def headerForTracking(headerName: String, headerValue: String): KafkaConsumeActionBuilder =
    headerForTracking(headerName, _ => Success(headerValue))

  def replyMatchBy(extractor: KafkaProtocolMessage => Array[Byte]): KafkaConsumeActionBuilder =
    this.modify(_.attributes.responseMatchExtractor).setTo(Some(extractor))

  def matchByMessage(extractor: KafkaProtocolMessage => Array[Byte]): KafkaConsumeActionBuilder =
    replyMatchBy(extractor)

  def matchByKafkaMatcher(matcher: KafkaMatcher): KafkaConsumeActionBuilder =
    replyMatchBy(matcher.responseMatch)

  def startTimeForTracking(startTime: Expression[Long]): KafkaConsumeActionBuilder =
    this.modify(_.attributes.startTimestamp).setTo(Some(startTime))

  def startTimeForTracking(sessionKey: String): KafkaConsumeActionBuilder =
    startTimeForTracking(session => session(sessionKey).validate[Long])

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
