package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.{Action, RequestAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.{KafkaMessageTracker, KafkaTrackerProvider}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaConsumeAttributes

object KafkaConsumeAction {
  private[kafka] def effectiveMessageMatcher(
      protocolMatcher: KafkaMatcher,
      attributes: KafkaConsumeAttributes,
  ): KafkaMatcher =
    if (attributes.expectedMatchId.isEmpty) {
      new KafkaMatcher {
        override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = KafkaConsumeAttributes.ConsumeAnyMatchId
        override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = KafkaConsumeAttributes.ConsumeAnyMatchId
      }
    } else
      attributes.responseMatchExtractor match {
        case Some(responseExtractor) =>
          new KafkaMatcher {
            override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = protocolMatcher.requestMatch(msg)
            override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = responseExtractor(msg)
          }
        case None                    => protocolMatcher
      }

  private[kafka] def effectiveConsumeSettings(
      components: KafkaComponents,
      attributes: KafkaConsumeAttributes,
  ): Map[String, AnyRef] =
    components.kafkaProtocol.consumeProperties ++ attributes.consumeSettingsOverride.getOrElse(Map.empty)

  private[kafka] def expectedMatchIdOrError(expectedMatchId: Array[Byte]): Either[String, Array[Byte]] =
    Option(expectedMatchId).toRight("consume-only matcher returned null expected match id")
}

class KafkaConsumeAction(
    components: KafkaComponents,
    attributes: KafkaConsumeAttributes,
    val statsEngine: StatsEngine,
    val clock: Clock,
    val next: Action,
    throttler: Option[Throttler],
) extends RequestAction with KafkaLogging with NameGen {
  override def requestName: Expression[String]   = attributes.requestName
  private val messageMatcher                     =
    KafkaConsumeAction.effectiveMessageMatcher(components.kafkaProtocol.messageMatcher, attributes)
  private val trackersPool: KafkaTrackerProvider =
    components.trackersPoolFactory.trackerProvider(KafkaConsumeAction.effectiveConsumeSettings(components, attributes))

  override def sendRequest(session: Session): Validation[Unit] =
    for {
      rn         <- requestName(session)
      topic      <- attributes.topic(session)
      expectedId <- attributes.expectedMatchId.map(_(session)).getOrElse(Success(KafkaConsumeAttributes.ConsumeAnyMatchId))
      startTime  <- attributes.startTimestamp.map(_(session)).getOrElse(Success(clock.nowMillis))
    } yield throttler
      .fold(trackAndAwait(rn, topic, expectedId, session, startTime))(
        _.throttle(session.scenario, () => trackAndAwait(rn, topic, expectedId, session, startTime)),
      )

  private def trackAndAwait(
      requestNameString: String,
      topic: String,
      expectedId: Array[Byte],
      session: Session,
      startTime: Long,
  ): Unit = {
    KafkaConsumeAction.expectedMatchIdOrError(expectedId) match {
      case Right(matchId)     =>
        val tracker: KafkaMessageTracker = trackersPool.tracker(topic, topic, messageMatcher, None)
        tracker.track(
          matchId,
          startTime,
          attributes.timeout.getOrElse(components.kafkaProtocol.timeout).toMillis,
          attributes.checks,
          attributes.replyExtractions,
          session,
          next,
          requestNameString,
          attributes.silent.getOrElse(false),
        )
      case Left(errorMessage) =>
        val now = clock.nowMillis
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

  override def name: String = genName("kafkaConsume")
}
