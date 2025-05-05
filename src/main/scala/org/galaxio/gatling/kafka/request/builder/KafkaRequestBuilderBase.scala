package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session._
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder
import org.galaxio.gatling.kafka.Predef._

import scala.reflect.ClassTag

object KafkaRequestBuilderBase {

  final case class OnlyPublishStep(requestName: Expression[String], producerTopic: Expression[String]) {
    def send[V: Serde: ClassTag](value: Expression[V]): RequestBuilder[Nothing, V] =
      KafkaRequestBuilder[Nothing, V](
        KafkaAttributes(
          requestName = requestName,
          producerTopic = Option(producerTopic),
          consumerTopic = None,
          key = None,
          value = value,
          headers = None,
          keySerde = None,
          valueSerde = implicitly[Serde[V]],
          checks = List.empty,
        ),
      )

    def send[K: Serde: ClassTag, V: Serde: ClassTag](
        key: Expression[K],
        value: Expression[V],
        headers: Expression[Headers] = List.empty[Header],
    ): RequestBuilder[K, V] =
      KafkaRequestBuilder(
        KafkaAttributes(
          requestName = requestName,
          producerTopic = Option(producerTopic),
          consumerTopic = None,
          key = Option(key),
          value = value,
          headers = Option(headers),
          keySerde = Option(implicitly[Serde[K]]),
          valueSerde = implicitly[Serde[V]],
          checks = List.empty,
        ),
      )
  }

}

case class KafkaRequestBuilderBase(requestName: Expression[String]) {

  def send[K: Serde: ClassTag, V: Serde: ClassTag](
      key: Expression[K],
      payload: Expression[V],
      headers: Expression[Headers] = List.empty[Header],
  ): RequestBuilder[K, V] = {
    if (key == null)
      KafkaRequestBuilder[Nothing, V](
        KafkaAttributes(
          requestName = requestName,
          producerTopic = None, // TODO: it should be set after topic definition
          consumerTopic = None,
          key = None,
          value = payload,
          headers = Option(headers),
          keySerde = None,
          valueSerde = implicitly[Serde[V]],
          checks = List.empty,
        ),
      )
    else
      KafkaRequestBuilder(
        KafkaAttributes(
          requestName = requestName,
          producerTopic = None, // TODO: it should be set after topic definition
          consumerTopic = None,
          key = Option(key),
          value = payload,
          headers = Option(headers),
          keySerde = Some(implicitly[Serde[K]]),
          valueSerde = implicitly[Serde[V]],
          checks = List.empty,
        ),
      )
  }

  def send[V: Serde: ClassTag](payload: Expression[V]): RequestBuilder[Nothing, V] =
    KafkaRequestBuilder[Nothing, V](
      KafkaAttributes(
        requestName = requestName,
        producerTopic = None, // TODO: it should be set after topic definition
        consumerTopic = None,
        key = None,
        value = payload,
        headers = None,
        keySerde = None,
        valueSerde = implicitly[Serde[V]],
        checks = List.empty,
      ),
    )

  def topic(producerTopic: Expression[String]): KafkaRequestBuilderBase.OnlyPublishStep =
    KafkaRequestBuilderBase.OnlyPublishStep(requestName, producerTopic)

  def requestReply: ReqRepBase.type = ReqRepBase

  object ReqRepBase {
    case class RROutTopicStep(producerTopic: Expression[String], consumerTopic: Expression[String]) {
      def send[K: Serde: ClassTag, V: Serde: ClassTag](
          key: Expression[K],
          payload: Expression[V],
          headers: Expression[Headers] = List.empty[Header].expressionSuccess,
      ): KafkaRequestReplyActionBuilder[K, V] = {
        KafkaRequestReplyActionBuilder[K, V](
          KafkaAttributes[K, V](
            requestName,
            Option(producerTopic),
            Option(consumerTopic),
            Option(key),
            payload,
            Option(headers),
            Option(implicitly[Serde[K]]),
            implicitly[Serde[V]],
            List.empty,
          ),
        )
      }
    }

    case class RRInTopicStep(producerTopic: Expression[String]) {
      def replyTopic(consumerTopic: Expression[String]): RROutTopicStep = RROutTopicStep(producerTopic, consumerTopic)
    }
    def requestTopic(rt: Expression[String]): RRInTopicStep = RRInTopicStep(rt)

  }

}
