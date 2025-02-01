package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders

trait LowPriorSender {
  implicit def noSchemaSender[K, V]: Sender[K, V] =
    new Sender[K, V] {

      override def send(requestName: Expression[String], payload: Expression[V]): RequestBuilder[Nothing, V] =
        KafkaRequestBuilder[Nothing, V](KafkaAttributes(requestName, None, payload, Right(new RecordHeaders())))

      override def send(
          requestName: Expression[String],
          key: Option[Expression[K]],
          payload: Expression[V],
      ): RequestBuilder[K, V] =
        KafkaRequestBuilder[K, V](KafkaAttributes(requestName, key, payload, Right(new RecordHeaders())))

      override def send(
          requestName: Expression[String],
          key: Option[Expression[K]],
          payload: Expression[V],
          headers: Expression[String],
      ): RequestBuilder[K, V] =
        KafkaRequestBuilder[K, V](KafkaAttributes(requestName, key, payload, Left(headers)))

      override def send(
         requestName: Expression[String],
         key: Option[Expression[K]],
         payload: Expression[V],
         headers: Headers,
       ): RequestBuilder[K, V] =
        KafkaRequestBuilder[K, V](KafkaAttributes(requestName, key, payload, Right(headers)))
    }
}
