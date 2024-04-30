package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session._
import org.apache.kafka.common.header.{Header, Headers}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serde
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
    val payloadExpression = payload.expressionSuccess
    val headersExpression = headers.expressionSuccess
    val requestBuilder    =
      if (key == null)
        sender.send(requestName, None, payloadExpression, Some(headersExpression))
      else
        sender.send(requestName, Some(key.expressionSuccess), payloadExpression, Some(headersExpression))
    if (silent) requestBuilder.silent else requestBuilder
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
    if (silent) requestBuilder.silent else requestBuilder
  }

  def send[V](payload: Expression[V])(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    sender.send(requestName = requestName, key = None, payload = payload, headers = None)

  def send[V](
      payload: V,
      headers: Headers,
      silent: Boolean,
  )(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    send(payload.expressionSuccess, headers.expressionSuccess, silent)

  def send[V](payload: V)(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    send(payload, new RecordHeaders(), silent = false)

  def send[V](payload: V, headers: Headers)(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] =
    send(payload, headers, silent = false)

  def send[V](
      payload: Expression[V],
      headers: Expression[Headers],
      silent: Boolean,
  )(implicit sender: Sender[Nothing, V]): RequestBuilder[_, V] = {
    val requestBuilder = sender.send(requestName = requestName, key = None, payload = payload, headers = Some(headers))
    if (silent) requestBuilder.silent else requestBuilder
  }

  def requestReply: ReqRepBase.type = ReqRepBase

  object ReqRepBase {
    case class RROutTopicStep(inputTopic: Expression[String], outputTopic: Expression[String]) {
      def send[K: Serde: ClassTag, V: Serde: ClassTag](
          key: Expression[K],
          payload: Expression[V],
          headers: Expression[Headers] = List.empty[Header].expressionSuccess,
      ): KafkaRequestReplyActionBuilder[K, V] = {
        KafkaRequestReplyActionBuilder[K, V](
          new KafkaRequestReplyAttributes[K, V](
            requestName = requestName,
            inputTopic = inputTopic,
            outputTopic = outputTopic,
            key = key,
            value = payload,
            headers = Some(headers),
            keySerializer = implicitly[Serde[K]].serializer(),
            valueSerializer = implicitly[Serde[V]].serializer(),
            checks = List.empty,
            silent = None,
          ),
        )
      }
    }

    case class RRInTopicStep(inputTopic: Expression[String]) {
      def replyTopic(outputTopic: Expression[String]): RROutTopicStep = RROutTopicStep(inputTopic, outputTopic)

    }
    def requestTopic(rt: Expression[String]): RRInTopicStep = RRInTopicStep(rt)

  }

}
