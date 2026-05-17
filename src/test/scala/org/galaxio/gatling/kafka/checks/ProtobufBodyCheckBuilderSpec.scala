package org.galaxio.gatling.kafka.checks

import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.KafkaProtobufSerdes
import org.galaxio.gatling.kafka.test.proto.test_messages.{TestRequest, TestResponse}
import org.scalatest.funsuite.AnyFunSuite

class ProtobufBodyCheckBuilderSpec extends AnyFunSuite with KafkaProtobufSerdes {

  test("scalapbSerde deserializes valid protobuf bytes to TestRequest") {
    val original = TestRequest(id = "req-1", payload = "hello", timestamp = 1000L)
    val bytes    = original.toByteArray
    val result   = TestRequest.parseFrom(bytes)
    assert(result == original)
  }

  test("scalapbSerde deserializes valid protobuf bytes to TestResponse") {
    val original = TestResponse(id = "resp-1", result = "success", success = true)
    val bytes    = original.toByteArray
    val result   = TestResponse.parseFrom(bytes)
    assert(result == original)
  }

  test("protobuf deserialization of empty bytes returns default instance") {
    val result = TestRequest.parseFrom(Array.emptyByteArray)
    assert(result == TestRequest.defaultInstance)
  }

  test("protobufBody check builder can be constructed for TestRequest") {
    val checkBuilder = ProtobufBodyCheckBuilder._protobufBody[TestRequest]
    assert(checkBuilder != null)
  }

  test("protobufBody check builder can be constructed for TestResponse") {
    val checkBuilder = ProtobufBodyCheckBuilder._protobufBody[TestResponse]
    assert(checkBuilder != null)
  }

  test("protobuf round-trip preserves all field types") {
    val original = TestRequest(id = "complex", payload = "special: !@#$%", timestamp = Long.MaxValue)
    val bytes    = original.toByteArray
    val result   = TestRequest.parseFrom(bytes)
    assert(result.id == "complex")
    assert(result.payload == "special: !@#$%")
    assert(result.timestamp == Long.MaxValue)
  }
}
