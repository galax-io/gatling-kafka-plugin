package org.galaxio.gatling.kafka.request.builder

import io.gatling.commons.validation.Validation
import io.gatling.commons.validation.Success
import io.gatling.core.session.Expression
import io.gatling.core.session.el._
import org.apache.kafka.common.serialization.Serde

import scala.reflect.{ClassTag, classTag}

object KafkaSerializedExpression {

  def apply[T: Serde: ClassTag](
      topic: Expression[String],
      value: Expression[T],
  ): Expression[Array[Byte]] = {
    val serializer = implicitly[Serde[T]].serializer()
    session =>
      if (classTag[T].runtimeClass.getCanonicalName == "java.lang.String")
        for {
          resolvedTopic <- topic(session)
          resolvedValue <- value
                             .asInstanceOf[Expression[String]](session)
                             .flatMap(_.el[String].apply(session))
        } yield serializer
          .asInstanceOf[org.apache.kafka.common.serialization.Serializer[String]]
          .serialize(resolvedTopic, resolvedValue)
      else
        for {
          resolvedTopic <- topic(session)
          resolvedValue <- value(session)
        } yield serializer.serialize(resolvedTopic, resolvedValue)
  }

  def static[T: Serde: ClassTag](topic: Expression[String], value: T): Expression[Array[Byte]] =
    apply(topic, _ => Success(value))
}
