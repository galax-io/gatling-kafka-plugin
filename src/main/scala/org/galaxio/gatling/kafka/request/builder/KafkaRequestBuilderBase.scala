package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session._
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.actions.KafkaConsumeActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder

import scala.reflect.ClassTag

case class KafkaRequestBuilderBase(requestName: Expression[String]) {

  import org.galaxio.gatling.kafka.Predef._

  def send[K, V](
      key: K,
      payload: V,
      headers: Headers,
      silent: Boolean,
  )(implicit sender: Sender[K, V]): RequestBuilder[K, V] = {
    val requestBuilder =
      if (key == null)
        sender.send(requestName, None, payload.expressionSuccess, Some(headers.expressionSuccess))
      else
        sender.send(requestName, Some(key.expressionSuccess), payload.expressionSuccess, Some(headers.expressionSuccess))
    if (silent) requestBuilder.silent else requestBuilder.notSilent
  }

  def send[K, V](key: K, payload: V)(implicit sender: Sender[K, V]): RequestBuilder[K, V] =
    send(key, payload, new RecordHeaders(), silent = false)

  def send[K, V](key: K, payload: V, headers: Headers)(implicit sender: Sender[K, V]): RequestBuilder[K, V] =
    send(key, payload, headers, silent = false)

  def send[K, V](
      key: Expression[K],
      payload: Expression[V],
      headers: Expression[Headers] = List.empty[Header],
  )(implicit
      sender: Sender[K, V],
  ): RequestBuilder[K, V] = {
    if (key == null)
      sender.send(requestName, None, payload, Some(headers))
    else
      sender.send(requestName, Some(key), payload, Some(headers))
  }

  def send[K, V](
      key: Expression[K],
      payload: Expression[V],
      headers: Expression[Headers],
      silent: Boolean,
  )(implicit sender: Sender[K, V]): RequestBuilder[K, V] = {
    val requestBuilder = send(key, payload, headers)
    if (silent) requestBuilder.silent else requestBuilder.notSilent
  }

  def send[V](payload: Expression[V])(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    sender.send(requestName, None, payload)

  def send[V](payload: V)(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    send(payload, new RecordHeaders(), silent = false)

  def send[V](payload: V, headers: Headers)(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    send(payload, headers, silent = false)

  def send[V](payload: V, headers: Headers, silent: Boolean)(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] = {
    val requestBuilder = sender.send(requestName, None, payload.expressionSuccess, Some(headers.expressionSuccess))
    if (silent) requestBuilder.silent else requestBuilder.notSilent
  }

  def send[V](
      payload: Expression[V],
      headers: Expression[Headers],
      silent: Boolean,
  )(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] = {
    val requestBuilder = sender.send(requestName, None, payload, Some(headers))
    if (silent) requestBuilder.silent else requestBuilder.notSilent
  }

  def requestReply: ReqRepBase.type = ReqRepBase

  def consumeFrom(topic: Expression[String]): KafkaConsumeActionBuilder = KafkaConsumeActionBuilder.create(requestName, topic)

  def consumeFrom(topic: String): KafkaConsumeActionBuilder = consumeFrom(topic.expressionSuccess)

  object ReqRepBase {
    case class RROutTopicStep(inputTopic: Expression[String], outputTopic: Expression[String]) {
      def send[K: Serde: ClassTag, V: Serde: ClassTag](
          key: Expression[K],
          payload: Expression[V],
          headers: Expression[Headers] = List.empty[Header].expressionSuccess,
      ): KafkaRequestReplyActionBuilder[K, V] = {
        KafkaRequestReplyActionBuilder[K, V](
          new KafkaRequestReplyAttributes[K, V](
            requestName,
            inputTopic,
            outputTopic,
            key,
            payload,
            Some(headers),
            implicitly[Serde[K]].serializer(),
            implicitly[Serde[V]].serializer(),
            List.empty,
            None,
            None,
            None,
            None,
            None,
            List.empty,
          ),
        )
      }
    }

    case class RRInTopicStep(inputTopic: Expression[String]) {
      def replyTopic(outputTopic: Expression[String]): RROutTopicStep = RROutTopicStep(inputTopic, outputTopic)
    }
    def requestTopic(rt: Expression[String]): RRInTopicStep = RRInTopicStep(rt)

  }
  def consumeAny(topic: Expression[String]): KafkaConsumeActionBuilder = consumeFrom(topic)

  def consumeAny(topic: String): KafkaConsumeActionBuilder = consumeAny(topic.expressionSuccess)

}
