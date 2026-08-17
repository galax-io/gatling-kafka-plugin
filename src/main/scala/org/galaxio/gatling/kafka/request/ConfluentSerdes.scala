package org.galaxio.gatling.kafka.request

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters._

/** Every construction of a Confluent Schema Registry type in the Scala sources lives here.
  *
  * The `io.confluent` artifacts are not published to Maven Central and are therefore optional: consumers declare them alongside
  * the Confluent resolver only if they want Schema-Registry-backed Avro. That is only workable if nothing on the path a *plain*
  * simulation takes constructs one of these types, and `Predef` is on every simulation's path.
  *
  * `KafkaSerdesImplicits.avroSerde` and `javaapi.checks.KafkaChecks.avroSerde` are both `def`s delegating to [[newAvroSerde]],
  * so nothing here runs while an entry point initialises. Until 2.0.0 a `LazyGenericAvroSerde` wrapper deferred the
  * construction instead, because the 1.x binary freeze forced `avroSerde` to be a strict `val`.
  *
  * The Java facade still names Confluent types directly in `javaapi.KafkaDsl` and `javaapi.request.expressions.Builders`; those
  * are public signatures under Principle I and cannot move. They are safe because the JVM resolves method descriptors lazily,
  * and `PlainClasspathIsolationSpec` covers them.
  *
  * Contract E1 of `specs/005-classpath-dependency-shedding/contracts/dsl-entry-points.md`.
  */
object ConfluentSerdes {

  /** A fresh serde for generic Avro records, resolving schemas through the Schema Registry client configured on the surrounding
    * Kafka properties.
    *
    * A `def`, not a `val`: each caller gets its own instance, because `GenericAvroSerde` carries mutable `configure`/`close`
    * state and the Scala DSL and the Java facade have always held separate ones.
    */
  def newAvroSerde(): Serde[GenericRecord] = new GenericAvroSerde()

  /** Serde for a type whose schema is registered in the Schema Registry at `schemaRegUrl`.
    *
    * @param schemaRegUrl
    *   comma-separated Schema Registry base URLs
    */
  def schemaRegistrySerde[T](schemaRegUrl: String): Serde[T] = new Serde[T] {
    // One client per serde, not one per accessor call. `serializer()` and `deserializer()` are invoked per message
    // (KafkaAction.serializeKey/serializeValue, AvroBodyCheckBuilder, KafkaMessagePreparer), so a `def` here handed every
    // record a client whose schema cache was empty by construction — turning the "Cached" client into a Schema Registry
    // round trip per record, inside the hot path of a load generator. CachedSchemaRegistryClient is thread-safe.
    private lazy val client: CachedSchemaRegistryClient =
      new CachedSchemaRegistryClient(schemaRegUrl.split(',').toList.asJava, 16)

    override def serializer(): Serializer[T] =
      new KafkaAvroSerializer(client).asInstanceOf[Serializer[T]]

    override def deserializer(): Deserializer[T] =
      new KafkaAvroDeserializer(client).asInstanceOf[Deserializer[T]]
  }
}
