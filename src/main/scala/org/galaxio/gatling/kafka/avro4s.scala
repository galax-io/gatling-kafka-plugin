package org.galaxio.gatling.kafka

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, Decoder, Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes}

import java.io.ByteArrayOutputStream

object avro4s {
  private final class Avro4sGenericSerializer[T: Encoder] extends Serializer[T] {
    override def serialize(topic: String, data: T): Array[Byte] =
      if (data == null)
        null
      else {
        val baos = new ByteArrayOutputStream()

        val avroOutputStream = AvroOutputStream.binary[T]
        val output           = avroOutputStream.to(baos).build()
        output.write(data)
        output.close()
        baos.toByteArray
      }
  }

  private final class Avro4sGenericDeserializer[T: Decoder](schema: Schema) extends Deserializer[T] {
    override def deserialize(topic: String, data: Array[Byte]): T =
      if (data == null)
        null.asInstanceOf[T]
      else {
        val avroInputStream = AvroInputStream.binary[T]
        val input           = avroInputStream.from(data).build(schema)
        val result          = input.iterator.next()
        input.close()
        result
      }
  }

  implicit def serdeAvro4s[T: SchemaFor: Encoder: Decoder]: Serde[T] = {
    val schemaFor = implicitly[SchemaFor[T]]
    Serdes.serdeFrom(
      new Avro4sGenericSerializer[T],
      new Avro4sGenericDeserializer[T](schemaFor.schema),
    )
  }
}
