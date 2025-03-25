package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.{Action, RequestAction}
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.el._
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.MessagePublished
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaRequestReplyAttributes

import scala.reflect.{ClassTag, classTag}

class KafkaRequestReplyAction[K: ClassTag, V: ClassTag](
    components: KafkaComponents,
    attributes: KafkaRequestReplyAttributes[K, V],
    val statsEngine: StatsEngine,
    val clock: Clock,
    val next: Action,
    throttler: Option[ActorRef[Throttler.Command]],
) extends RequestAction with KafkaLogging with NameGen {
  override def requestName: Expression[String] = attributes.requestName

  override def sendRequest(session: Session): Validation[Unit] = {
    for {
      rn            <- requestName(session)
      msg           <- resolveToProtocolMessage(session)
      trackingTopic <- attributes.outputTopic(session)
    } yield throttler
      .fold(publishAndLogMessage(rn, trackingTopic, msg, session))(
        _ ! Throttler.Command.ThrottledRequest(session.scenario, () => publishAndLogMessage(rn, trackingTopic, msg, session)),
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
        key     <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        topic   <- attributes.inputTopic(s)
        value   <- attributes.value
                     .asInstanceOf[Expression[String]](s)
                     .flatMap(_.el[String].apply(s))
                     .map(v => attributes.valueSerializer.asInstanceOf[Serializer[String]].serialize(topic, v))
        headers <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, topic, headers)
    else
      for {
        key     <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        topic   <- attributes.inputTopic(s)
        value   <- attributes.value(s).map(v => attributes.valueSerializer.serialize(topic, v))
        headers <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, topic, headers)

  private def publishAndLogMessage(
      requestNameString: String,
      trackingTopic: String,
      msg: KafkaProtocolMessage,
      session: Session,
  ): Unit = {
    val now = clock.nowMillis
    components.sender.send(msg)(
      rm => {
        if (logger.underlying.isDebugEnabled) {
          logMessage(s"Record sent user=${session.userId} key=${new String(msg.key)} topic=${rm.topic()}", msg)
        }
        val id      = components.kafkaProtocol.messageMatcher.requestMatch(msg)
        // Notify tracker that we are sending an Event
        // N.B the topic here is the output topic (this is for request reply, so we always
        // want the tracker on the output topic)
        val tracker =
          components.trackersPool.tracker(trackingTopic, components.kafkaProtocol.messageMatcher, None)
        tracker ! MessagePublished(
          id,
          clock.nowMillis,
          components.kafkaProtocol.timeout.toMillis,
          attributes.checks,
          session,
          next,
          requestNameString,
        )
      },
      e => {
        logger.error(e.getMessage, e)
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
      },
    )
  }

  override def name: String = genName("kafkaRequestReply")
}
