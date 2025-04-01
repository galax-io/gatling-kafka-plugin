package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.el._
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.MessagePublished
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender, KafkaSenderImpl}
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
    val throttler: Option[ActorRef[Throttler.Command]],
) extends KafkaBaseAction[K, V] {

  override def requestName: Expression[String] = attributes.requestName

  private val kafkaSender: KafkaSender[Array[Byte], Array[Byte]] =
    new KafkaSenderImpl(components.kafkaProtocol.producerProperties, components.coreComponents)

  private val trackersPool = new KafkaMessageTrackerPool(
    components.kafkaProtocol.consumerProperties,
    components.coreComponents.actorSystem,
    components.coreComponents.statsEngine,
    components.coreComponents.clock,
  )

  override def sendRequest(session: Session): Validation[Unit] = {
    for {
      rn            <- requestName(session)
      msg           <- resolveToProtocolMessage(session)
      trackingTopic <- attributes.outputTopic(session)
    } yield throttler match {
      case Some(th) =>
        th ! Throttler.Command.ThrottledRequest(
          session.scenario,
          () => publishAndLogMessage(rn, trackingTopic, msg, session),
        )
      case _        => publishAndLogMessage(rn, trackingTopic, msg, session)
    }
  }

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
    kafkaSender
      .send(
        protocolMessage = msg.toProducerRecord,
        onSuccess = _ => {
          val id = components.kafkaProtocol.messageMatcher.requestMatch(msg)
          if (logger.underlying.isDebugEnabled) {
            logMessage(s"Record sent user=${session.userId} key=${new String(msg.key)} topic=$trackingTopic", msg)
          }

          // Notify tracker that we are sending an Event
          // N.B the topic here is the output topic (this is for request reply, so we always
          // want the tracker on the output topic)
          val tracker =
            trackersPool.tracker(trackingTopic, components.kafkaProtocol.messageMatcher, None)
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
        onFailure = exception => {
          logger.error(exception.getMessage, exception)
          statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestNameString,
            now,
            clock.nowMillis,
            KO,
            Some("500"),
            Some(exception.getMessage),
          )
        },
      )
  }

  override def name: String = genName("kafkaRequestReply")
}
