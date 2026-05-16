package org.galaxio.gatling.kafka.request.builder

import io.gatling.commons.validation.Success
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.Predef._
import org.scalatest.funsuite.AnyFunSuite

class KafkaSerializedExpressionSpec extends AnyFunSuite {

  test("serializes string expressions after resolving Gatling EL") {
    val topicExpression = (_: Session) => Success("events")
    val valueExpression = (_: Session) => Success("#{correlationId}")
    val session         = Session("scenario", 1L, null).set("correlationId", "corr-42")

    val expression = KafkaSerializedExpression[String](topicExpression, valueExpression)
    val result     = expression(session)

    assert(result.map(_.sameElements("corr-42".getBytes())) == Success(true))
  }

  test("serializes non-string values via configured serde") {
    val topicExpression = (_: Session) => Success("events")
    val valueExpression = (_: Session) => Success(42L)

    val expression = KafkaSerializedExpression[Long](topicExpression, valueExpression)
    val result     = expression(Session("scenario", 1L, null))

    assert(result.map(bytes => java.nio.ByteBuffer.wrap(bytes).getLong) == Success(42L))
  }
}
