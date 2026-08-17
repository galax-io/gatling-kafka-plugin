package org.galaxio.gatling.kafka.request

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes => JSerdes}

import java.nio.ByteBuffer
import java.util.UUID

trait KafkaSerdesImplicits {
  implicit def stringSerde: Serde[String]                             = JSerdes.String()
  implicit def longSerde: Serde[Long]                                 = JSerdes.Long().asInstanceOf[Serde[Long]]
  implicit def javaLongSerde: Serde[java.lang.Long]                   = JSerdes.Long()
  implicit def byteArraySerde: Serde[Array[Byte]]                     = JSerdes.ByteArray()
  implicit def bytesSerde: Serde[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes()
  implicit def byteBufferSerde: Serde[ByteBuffer]                     = JSerdes.ByteBuffer()
  implicit def shortSerde: Serde[Short]                               = JSerdes.Short().asInstanceOf[Serde[Short]]
  implicit def javaShortSerde: Serde[java.lang.Short]                 = JSerdes.Short()
  implicit def floatSerde: Serde[Float]                               = JSerdes.Float().asInstanceOf[Serde[Float]]
  implicit def javaFloatSerde: Serde[java.lang.Float]                 = JSerdes.Float()
  implicit def doubleSerde: Serde[Double]                             = JSerdes.Double().asInstanceOf[Serde[Double]]
  implicit def javaDoubleSerde: Serde[java.lang.Double]               = JSerdes.Double()
  implicit def intSerde: Serde[Int]                                   = JSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit def javaIntegerSerde: Serde[java.lang.Integer]             = JSerdes.Integer()
  implicit def uuidSerde: Serde[UUID]                                 = JSerdes.UUID()

  // Neither Avro member constructs a Confluent type while this trait — which `Predef` mixes in, and
  // every simulation imports — initialises. Their declared types (`Serde`, `GenericRecord`) come from
  // Maven Central-published artifacts, so implicit search over this trait works with no Confluent
  // artifact present; only actually summoning one touches Confluent, and a simulation that summons one
  // is by definition doing Avro and has the artifacts.
  //
  // `avroSerde` was a strict `val` until 2.0.0, deferring through a `LazyGenericAvroSerde` wrapper,
  // because turning a published concrete trait member `lazy` deletes the mixin setter from the
  // compiled interface — a simulation built against an earlier release would keep its own field, never
  // have it assigned by `$init$`, and silently read `null`. A major release is where that is paid for
  // openly, so the wrapper is gone and the member is simply `lazy`.
  //
  // `lazy val`, not `def`. `GenericAvroSerde` is unusable until `configure(configs, isKey)` supplies
  // its Schema Registry client, and nothing in the plugin calls that — configuring the serde the DSL
  // hands out is the user's only route. A `def` would hand every summon a fresh, unconfigured instance,
  // so configuring it would silently configure a throwaway. One instance per mixing-in object keeps
  // that route working while still constructing nothing until first use.

  implicit def serdeClass[T](implicit schemaRegUrl: String): Serde[T] =
    ConfluentSerdes.schemaRegistrySerde[T](schemaRegUrl)

  implicit lazy val avroSerde: Serde[GenericRecord] = ConfluentSerdes.newAvroSerde()

}
