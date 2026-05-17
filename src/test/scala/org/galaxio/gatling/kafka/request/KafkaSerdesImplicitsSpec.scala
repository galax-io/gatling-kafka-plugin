package org.galaxio.gatling.kafka.request

import org.apache.kafka.common.serialization.Serde
import org.scalatest.funsuite.AnyFunSuite

import java.nio.ByteBuffer
import java.util.UUID

class KafkaSerdesImplicitsSpec extends AnyFunSuite with KafkaSerdesImplicits {

  test("stringSerde round-trips string values") {
    val serde  = implicitly[Serde[String]]
    val bytes  = serde.serializer().serialize("topic", "hello")
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result == "hello")
  }

  test("longSerde round-trips Long values") {
    val serde  = implicitly[Serde[Long]]
    val bytes  = serde.serializer().serialize("topic", 42L)
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result == 42L)
  }

  test("intSerde round-trips Int values") {
    val serde  = implicitly[Serde[Int]]
    val bytes  = serde.serializer().serialize("topic", 123)
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result == 123)
  }

  test("doubleSerde round-trips Double values") {
    val serde  = implicitly[Serde[Double]]
    val bytes  = serde.serializer().serialize("topic", 3.14)
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result == 3.14)
  }

  test("byteArraySerde round-trips byte arrays") {
    val serde  = implicitly[Serde[Array[Byte]]]
    val data   = Array[Byte](1, 2, 3, 4)
    val bytes  = serde.serializer().serialize("topic", data)
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result.sameElements(data))
  }

  test("uuidSerde round-trips UUID values") {
    val serde = implicitly[Serde[UUID]]
    val id    = UUID.randomUUID()
    val bytes = serde.serializer().serialize("topic", id)
    assert(serde.deserializer().deserialize("topic", bytes) == id)
  }

  test("byteBufferSerde round-trips ByteBuffer values") {
    val serde  = implicitly[Serde[ByteBuffer]]
    val buf    = ByteBuffer.wrap(Array[Byte](10, 20, 30))
    val bytes  = serde.serializer().serialize("topic", buf)
    val result = serde.deserializer().deserialize("topic", bytes)
    assert(result.array().sameElements(Array[Byte](10, 20, 30)))
  }

  test("SchemaRegistryClientCache returns same instance for same URL") {
    val client1 = SchemaRegistryClientCache.getOrCreate("http://localhost:8081")
    val client2 = SchemaRegistryClientCache.getOrCreate("http://localhost:8081")
    assert(client1 eq client2)
  }

  test("SchemaRegistryClientCache returns different instances for different URLs") {
    val client1 = SchemaRegistryClientCache.getOrCreate("http://host-a:8081")
    val client2 = SchemaRegistryClientCache.getOrCreate("http://host-b:8081")
    assert(client1 ne client2)
  }
}
