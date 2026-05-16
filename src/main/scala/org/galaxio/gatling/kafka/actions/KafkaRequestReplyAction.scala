package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.{Action, RequestAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.el._
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.{KafkaMessageTracker, KafkaSender, KafkaTrackerProvider}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaMatcher, KafkaMessageMatcher}
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaRequestReplyAttributes

import scala.reflect.{ClassTag, classTag}

object KafkaRequestReplyAction {
  private[kafka] final case class PreparedTracker(
      matchId: Array[Byte],
      tracker: KafkaMessageTracker,
  )

  private[kafka] def requestMatchIdOrError(
      protocolMessage: KafkaProtocolMessage,
      messageMatcher: KafkaMatcher,
  ): Either[String, Array[Byte]] =
    Option(messageMatcher.requestMatch(protocolMessage)).toRight("request matcher returned null match id")

  private[kafka] def prepareTrackerOrError(
      protocolMessage: KafkaProtocolMessage,
      trackersPool: KafkaTrackerProvider,
      messageMatcher: KafkaMatcher,
  ): Either[String, PreparedTracker] =
    requestMatchIdOrError(protocolMessage, messageMatcher).map { matchId =>
      PreparedTracker(
        matchId,
        trackersPool.tracker(protocolMessage.inputTopic, protocolMessage.outputTopic, messageMatcher, None),
      )
    }

  private[kafka] def effectiveMessageMatcher(
      protocolMatcher: KafkaMatcher,
      attributes: KafkaRequestReplyAttributes[_, _],
  ): KafkaMatcher =
    new KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte] =
        attributes.requestMatchExtractor.getOrElse(protocolMatcher.requestMatch _)(msg)

      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] =
        attributes.responseMatchExtractor.getOrElse(protocolMatcher.responseMatch _)(msg)
    }

  private[kafka] def effectiveProducerSettings(
      components: KafkaComponents,
      attributes: KafkaRequestReplyAttributes[_, _],
  ): Map[String, AnyRef] =
    components.kafkaProtocol.producerProperties ++ attributes.producerSettingsOverride.getOrElse(Map.empty)

  private[kafka] def effectiveConsumeSettings(
      components: KafkaComponents,
      attributes: KafkaRequestReplyAttributes[_, _],
  ): Map[String, AnyRef] =
    components.kafkaProtocol.consumeProperties ++ attributes.consumeSettingsOverride.getOrElse(Map.empty)
}

class KafkaRequestReplyAction[K: ClassTag, V: ClassTag](
    components: KafkaComponents,
    attributes: KafkaRequestReplyAttributes[K, V],
    val statsEngine: StatsEngine,
    val clock: Clock,
    val next: Action,
    throttler: Option[Throttler],
) extends RequestAction with KafkaLogging with NameGen {
  override def requestName: Expression[String]   = attributes.requestName
  private val messageMatcher                     =
    KafkaRequestReplyAction.effectiveMessageMatcher(components.kafkaProtocol.messageMatcher, attributes)
  private val sender: KafkaSender                =
    components.senderProvider.sender(KafkaRequestReplyAction.effectiveProducerSettings(components, attributes))
  private val trackersPool: KafkaTrackerProvider =
    components.trackersPoolFactory.trackerProvider(KafkaRequestReplyAction.effectiveConsumeSettings(components, attributes))

  override def sendRequest(session: Session): Validation[Unit] = {
    for {
      rn        <- requestName(session)
      msg       <- resolveToProtocolMessage(session)
      startTime <- attributes.startTimestamp.map(_(session)).getOrElse(Success(clock.nowMillis))
    } yield throttler
      .fold(publishAndLogMessage(rn, msg, session, startTime))(
        _.throttle(session.scenario, () => publishAndLogMessage(rn, msg, session, startTime)),
      )

  }

  private def serializeKey(
      serializer: Serializer[K],
      keyE: Expression[K],
      topicE: Expression[String],
  ): Expression[Array[Byte]] = s =>
    // need for work gatling Expression Language
    if (classTag[K].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        topic <- topicE(s)
        key   <- keyE.asInstanceOf[Expression[String]](s).flatMap(_.el[String].apply(s))
      } yield serializer.asInstanceOf[Serializer[String]].serialize(topic, key)
    else
      for {
        topic <- topicE(s)
        key   <- keyE(s)
      } yield serializer.serialize(topic, key)

  private def optToVal[T](ovt: Option[Validation[T]]): Validation[Option[T]] =
    ovt.fold(Option.empty[T].success)(_.map(Option[T]))

  private def resolveToProtocolMessage: Expression[KafkaProtocolMessage] = s =>
    // need for work gatling Expression Language
    if (classTag[V].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        key         <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        inputTopic  <- attributes.inputTopic(s)
        outputTopic <- attributes.outputTopic(s)
        value       <- attributes.value
                         .asInstanceOf[Expression[String]](s)
                         .flatMap(_.el[String].apply(s))
                         .map(v => attributes.valueSerializer.asInstanceOf[Serializer[String]].serialize(inputTopic, v))
        headers     <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, inputTopic, outputTopic, headers)
    else
      for {
        key         <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        inputTopic  <- attributes.inputTopic(s)
        outputTopic <- attributes.outputTopic(s)
        value       <- attributes.value(s).map(v => attributes.valueSerializer.serialize(inputTopic, v))
        headers     <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, inputTopic, outputTopic, headers)

  private def publishAndLogMessage(
      requestNameString: String,
      msg: KafkaProtocolMessage,
      session: Session,
      startTime: Long,
  ): Unit = {
    val now = clock.nowMillis
    KafkaRequestReplyAction.prepareTrackerOrError(msg, trackersPool, messageMatcher) match {
      case Right(preparedTracker) =>
        sender.send(msg)(
          rm => {
            if (logger.underlying.isDebugEnabled) {
              logMessage(s"Record sent user=${session.userId} key=${new String(msg.key)} topic=${rm.topic()}", msg)
            }
            preparedTracker.tracker
              .track(
                preparedTracker.matchId,
                startTime,
                components.kafkaProtocol.timeout.toMillis,
                attributes.checks,
                attributes.replyExtractions,
                session,
                next,
                requestNameString,
                attributes.silent.getOrElse(false),
              )
          },
          e => {
            logger.error(e.getMessage, e)
            if (!attributes.silent.getOrElse(false)) {
              statsEngine.logResponse(
                session.scenario,
                session.groups,
                requestNameString,
                now,
                clock.nowMillis,
                KO,
                Some("500"),
                Some(e.getMessage),
              )
            }
          },
        )
      case Left(errorMessage)     =>
        logger.error(errorMessage)
        if (!attributes.silent.getOrElse(false)) {
          statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestNameString,
            now,
            clock.nowMillis,
            KO,
            Some("500"),
            Some(errorMessage),
          )
        }
        next ! session.logGroupRequestTimings(now, clock.nowMillis).markAsFailed
    }
  }

  override def name: String = genName("kafkaRequestReply")
}
