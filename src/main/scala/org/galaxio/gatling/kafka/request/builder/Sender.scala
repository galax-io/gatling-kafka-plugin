package org.galaxio.gatling.kafka.request.builder

import com.sksamuel.avro4s.{FromRecord, RecordFormat, SchemaFor}
import io.gatling.core.session.Expression
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders

trait Sender[K, V] {

  def send(requestName: Expression[String], payload: Expression[V]): RequestBuilder[Nothing, V]

  def send(requestName: Expression[String], key: Option[Expression[K]], payload: Expression[V]): RequestBuilder[K, V]

  def send(
      requestName: Expression[String],
      key: Option[Expression[K]],
      payload: Expression[V],
      headers: Expression[String],
  ): RequestBuilder[K, V]

  def send(
      requestName: Expression[String],
      key: Option[Expression[K]],
      payload: Expression[V],
      headers: Headers,
  ): RequestBuilder[K, V]

}

object Sender extends LowPriorSender {

  implicit def Avro4sSender[K, V](implicit
      schema: SchemaFor[V],
      format: RecordFormat[V],
      fromRecord: FromRecord[V],
  ): Sender[K, V] = new Sender[K, V] {

    override def send(requestName: Expression[String], payload: Expression[V]): RequestBuilder[Nothing, V] =
      new KafkaAvro4sRequestBuilder[Nothing, V](
        Avro4sAttributes(requestName, None, payload, schema, format, fromRecord, Right(new RecordHeaders())),
      )

    override def send(
        requestName: Expression[String],
        key: Option[Expression[K]],
        payload: Expression[V],
    ): RequestBuilder[K, V] =
      new KafkaAvro4sRequestBuilder[K, V](
        Avro4sAttributes(requestName, key, payload, schema, format, fromRecord, Right(new RecordHeaders())),
      )

    override def send(
        requestName: Expression[String],
        key: Option[Expression[K]],
        payload: Expression[V],
        headers: Expression[String],
    ): RequestBuilder[K, V] =
      new KafkaAvro4sRequestBuilder[K, V](
        Avro4sAttributes(requestName, key, payload, schema, format, fromRecord, Left(headers)),
      )

    override def send(
        requestName: Expression[String],
        key: Option[Expression[K]],
        payload: Expression[V],
        headers: Headers,
    ): RequestBuilder[K, V] =
      new KafkaAvro4sRequestBuilder[K, V](
        Avro4sAttributes(requestName, key, payload, schema, format, fromRecord, Right(headers)),
      )

  }

}
