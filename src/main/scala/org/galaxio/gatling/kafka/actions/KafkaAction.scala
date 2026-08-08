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
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import scala.reflect.{ClassTag, classTag}

abstract class KafkaAction[K: ClassTag, V: ClassTag](
    attributes: KafkaAttributes[K, V],
    throttler: Option[ActorRef[Throttler.Command]],
) extends RequestAction with KafkaLogging with NameGen {

  private val missingProducerTopicError =
    "Kafka producer topic is not defined. Set it with kafka(\"request\").topic(...)."

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

  private def resolveProducerTopic(session: Session): Validation[String] =
    attributes.producerTopic
      .map(_(session))
      .getOrElse(missingProducerTopicError.failure)

  private val isStringType: Boolean = classTag[V].runtimeClass.getCanonicalName == "java.lang.String"
  private val isKeyString: Boolean  = classTag[K].runtimeClass.getCanonicalName == "java.lang.String"

  private def serializeKey(
      serde: Option[Serde[? <: K]],
      keyExpression: Option[Expression[? <: K]],
      topic: String,
      session: Session,
  ): Validation[Option[Array[Byte]]] =
    if (isKeyString)
      traverse(for {
        serializer <- serde.asInstanceOf[Option[Serde[String]]].map(_.serializer())
        key        <- keyExpression.asInstanceOf[Option[Expression[String]]].map(_(session))
        keyEl       = key.flatMap(_.el[String].apply(session))
      } yield keyEl.map(serializer.serialize(topic, _)))
    else
      traverse(for {
        serializer <- serde.map(_.serializer().asInstanceOf[Serializer[K]])
        key        <- keyExpression.map(_(session))
      } yield key.map(serializer.serialize(topic, _)))

  private def serializeValue(topic: String, session: Session): Validation[Array[Byte]] =
    if (isStringType)
      attributes.value
        .asInstanceOf[Expression[String]](session)
        .flatMap(_.el[String].apply(session))
        .map(v => attributes.valueSerde.asInstanceOf[Serde[String]].serializer().serialize(topic, v))
    else
      attributes
        .value(session)
        .map(v => attributes.valueSerde.serializer().asInstanceOf[Serializer[V]].serialize(topic, v))

  private def resolveToProtocolMessage: Expression[KafkaProtocolMessage] = s =>
    for {
      producerTopic <- resolveProducerTopic(s)
      key           <- serializeKey(attributes.keySerde, attributes.key, producerTopic, s)
      consumerTopic <- traverse(attributes.consumerTopic.map(_(s)))
      value         <- serializeValue(producerTopic, s)
      headers       <- traverse(attributes.headers.map(_(s)))
    } yield KafkaProtocolMessage(
      // `orNull`, not an empty array. Substituting one collapsed two distinct states into one and cost
      // both correctness and realism (issue #167):
      //
      //   - Correlation: an empty key is a value, and every keyless request produced the same one, so
      //     they all shared a single slot in the tracker's correlation table. A reply resolved whichever
      //     request happened to occupy it, crediting one virtual user with another's answer.
      //   - The wire: an empty array is a *present* key. Kafka hashes it, and murmur2 of an empty input is
      //     constant, so keyless records could never be placed as keyless records — and a log-compacted
      //     topic, which requires a key, accepted records it should have rejected. How the broker places a
      //     genuinely keyless record is its own decision and has changed across versions, so the plugin
      //     asserts the key is absent and asserts nothing about placement.
      //
      // `KafkaProtocolMessage.key` has always been nullable on the consume side — `from` copies
      // `consumerRecord.key()` verbatim, which is null for a keyless record. This makes the produce side
      // agree with it, so no signature changes.
      key.orNull,
      value,
      producerTopic,
      consumerTopic.getOrElse(producerTopic),
      headers,
    )

  def sendKafkaMessage(requestNameString: String, protocolMessage: KafkaProtocolMessage, session: Session): Unit
}
