package org.galaxio.gatling.kafka.request

import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.test.proto.test_messages.{TestRequest, TestResponse}
import org.scalatest.funsuite.AnyFunSuite

class KafkaProtobufSerdesSpec extends AnyFunSuite with KafkaProtobufSerdes {

  test("scalapbSerde round-trips TestRequest") {
    val serde   = implicitly[Serde[TestRequest]]
    val request = TestRequest(id = "req-1", payload = "hello", timestamp = 12345L)
    val bytes   = serde.serializer().serialize("topic", request)
    val result  = serde.deserializer().deserialize("topic", bytes)
    assert(result == request)
  }

  test("scalapbSerde round-trips TestResponse") {
    val serde    = implicitly[Serde[TestResponse]]
    val response = TestResponse(id = "resp-1", result = "done", success = true)
    val bytes    = serde.serializer().serialize("topic", response)
    val result   = serde.deserializer().deserialize("topic", bytes)
    assert(result == response)
  }

  test("scalapbSerde serializer handles null") {
    val serde = implicitly[Serde[TestRequest]]
    val bytes = serde.serializer().serialize("topic", null.asInstanceOf[TestRequest])
    assert(bytes == null)
  }

  test("scalapbSerde deserializer handles null") {
    val serde  = implicitly[Serde[TestRequest]]
    val result = serde.deserializer().deserialize("topic", null)
    assert(result == null)
  }

  test("scalapbSerde preserves all fields") {
    val serde   = implicitly[Serde[TestRequest]]
    val request = TestRequest(id = "complex-id", payload = "data with spaces & special chars!", timestamp = Long.MaxValue)
    val bytes   = serde.serializer().serialize("topic", request)
    val result  = serde.deserializer().deserialize("topic", bytes)
    assert(result.id == "complex-id")
    assert(result.payload == "data with spaces & special chars!")
    assert(result.timestamp == Long.MaxValue)
  }
}
