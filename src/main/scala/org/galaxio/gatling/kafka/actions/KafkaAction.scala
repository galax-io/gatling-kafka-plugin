package org.galaxio.gatling.kafka.actions

import io.gatling.commons.validation._
import io.gatling.core.action.RequestAction
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.session.el._
import io.gatling.core.util.NameGen
import org.apache.kafka.common.serialization.{Serde, Serializer}
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.protocol.KafkaComponents
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.reflect.{ClassTag, classTag}

abstract class KafkaAction[K: ClassTag, V: ClassTag](
    components: KafkaComponents, // TODO: remove it after 1.1.0 (when topic removed from protocol)
    attributes: KafkaAttributes[K, V],
    throttler: Option[ActorRef[Throttler.Command]],
) extends RequestAction with KafkaLogging with NameGen {

  override def requestName: Expression[String] = attributes.requestName

  override def sendRequest(session: Session): Validation[Unit] = {
    for {
      requestNameString <- requestName(session)
      protocolMessage   <- resolveToProtocolMessage(session)
    } yield throttler
      .fold(sendKafkaMessage(requestNameString, protocolMessage, session))(
        _ ! Throttler.Command
          .ThrottledRequest(session.scenario, () => sendKafkaMessage(requestNameString, protocolMessage, session)),
      )

  }

  private def traverse[T](ovt: Option[Validation[T]]): Validation[Option[T]] =
    ovt.fold(Option.empty[T].success)(_.map(Option[T]))

  private def serializeKey(
      serde: Option[Serde[? <: K]],
      keyExpression: Option[Expression[? <: K]],
      topicExpression: Expression[String],
  ): Expression[Option[Array[Byte]]] = session =>
    // need for work gatling Expression Language
    if (classTag[K].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        topic  <- topicExpression(session)
        result <- traverse(for {
                    serializer <- serde.asInstanceOf[Option[Serde[String]]].map(_.serializer())
                    key        <- keyExpression.asInstanceOf[Option[Expression[String]]].map(_(session))
                    keyEl       = key.flatMap(_.el[String].apply(session))
                  } yield keyEl.map(serializer.serialize(topic, _)))
      } yield result
    else
      for {
        topic  <- topicExpression(session)
        result <- traverse(for {
                    serializer <- serde.map(_.serializer().asInstanceOf[Serializer[K]])
                    key        <- keyExpression.map(_(session))
                  } yield key.map(serializer.serialize(topic, _)))
      } yield result

  private def resolveToProtocolMessage: Expression[KafkaProtocolMessage] = s =>
    // need for work gatling Expression Language
    if (classTag[V].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        key           <- serializeKey(
                           attributes.keySerde,
                           attributes.key,
                           attributes.producerTopic.getOrElse(components.kafkaProtocol.producerTopic.el),
                         )(s)
        producerTopic <- attributes.producerTopic.fold(components.kafkaProtocol.producerTopic.success)(_(s))
        consumerTopic <- traverse(attributes.consumerTopic.map(_(s)))
        value         <- attributes.value
                           .asInstanceOf[Expression[String]](s)
                           .flatMap(_.el[String].apply(s))
                           .map(v => attributes.valueSerde.asInstanceOf[Serde[String]].serializer().serialize(producerTopic, v))
        headers       <- traverse(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(
        key.getOrElse(Array.emptyByteArray),
        value,
        producerTopic,
        consumerTopic.getOrElse("<unknown>"),
        headers,
      )
    else
      for {
        key           <- serializeKey(
                           attributes.keySerde,
                           attributes.key,
                           attributes.producerTopic.getOrElse(components.kafkaProtocol.producerTopic.el),
                         )(s)
        producerTopic <- attributes.producerTopic.fold(components.kafkaProtocol.producerTopic.success)(_(s))
        consumerTopic <- traverse(attributes.consumerTopic.map(_(s)))
        value         <- attributes
                           .value(s)
                           .map(v => attributes.valueSerde.serializer().asInstanceOf[Serializer[V]].serialize(producerTopic, v))
        headers       <- traverse(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(
        key.getOrElse(Array.emptyByteArray),
        value,
        producerTopic,
        consumerTopic.getOrElse("<unknown>"),
        headers,
      )

  def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit
}
