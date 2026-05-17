package org.galaxio.gatling.kafka.request

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}
import org.apache.kafka.streams.kstream.WindowedSerdes

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

object SchemaRegistryClientCache {
  private val clients = new ConcurrentHashMap[String, CachedSchemaRegistryClient]()

  private val DefaultCacheCapacity = 128

  def getOrCreate(urls: String, cacheCapacity: Int = DefaultCacheCapacity): CachedSchemaRegistryClient =
    clients.computeIfAbsent(
      urls,
      u => new CachedSchemaRegistryClient(u.split(',').toList.asJava, cacheCapacity),
    )

  def getOrCreate(
      urls: String,
      configs: java.util.Map[String, ?],
      cacheCapacity: Int,
  ): CachedSchemaRegistryClient =
    clients.computeIfAbsent(
      urls,
      u => new CachedSchemaRegistryClient(u.split(',').toList.asJava, cacheCapacity, configs),
    )
}

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

  implicit def sessionWindowedSerde[T](implicit tSerde: Serde[T]): WindowedSerdes.SessionWindowedSerde[T] =
    new WindowedSerdes.SessionWindowedSerde[T](tSerde)

  implicit def serdeClass[T](implicit schemaRegUrl: String): Serde[T] = {
    val client = SchemaRegistryClientCache.getOrCreate(schemaRegUrl)
    new Serde[T] {
      override def serializer(): Serializer[T] =
        new KafkaAvroSerializer(client).asInstanceOf[Serializer[T]]

      override def deserializer(): Deserializer[T] =
        new KafkaAvroDeserializer(client).asInstanceOf[Deserializer[T]]
    }
  }

  implicit def avroSerde(implicit schemaRegUrl: String): Serde[GenericRecord] = {
    val client = SchemaRegistryClientCache.getOrCreate(schemaRegUrl)
    JSerdes.serdeFrom(
      new KafkaAvroSerializer(client).asInstanceOf[Serializer[GenericRecord]],
      new KafkaAvroDeserializer(client).asInstanceOf[Deserializer[GenericRecord]],
    )
  }

}
