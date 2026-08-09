package org.galaxio.gatling.kafka.request

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util
import scala.jdk.CollectionConverters._

/** Every construction of a Confluent Schema Registry type in the Scala sources lives here.
  *
  * The `io.confluent` artifacts are not published to Maven Central and are therefore optional: consumers declare them alongside
  * the Confluent resolver only if they want Schema-Registry-backed Avro. That is only workable if nothing on the path a *plain*
  * simulation takes constructs one of these types, and `Predef` is on every simulation's path.
  *
  * `LazyGenericAvroSerde` is what lets the DSL keep handing out a `Serde[GenericRecord]` from a strict `val` — which the
  * published trait ABI requires — without that val's construction touching Confluent.
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

/** A `Serde[GenericRecord]` that builds its Confluent-backed delegate on first use rather than on construction.
  *
  * This exists to satisfy two requirements that would otherwise conflict:
  *
  *   - Constitution Principle I: `KafkaSerdesImplicits.avroSerde` is a published concrete trait member. Turning it into a
  *     `lazy val` deletes the mixin setter from the interface, so a simulation compiled against an earlier release keeps its
  *     own field, never has it assigned, and silently observes `null` — with no linkage error to point at the cause. It must
  *     stay a strict `val`.
  *   - Contract E1: initialising `Predef` must not construct a Confluent type, or the optional artifacts become mandatory for
  *     every simulation, Avro or not.
  *
  * Constructing one of these does neither: the constructor is empty and the only reference to `ConfluentSerdes` sits in a lazy
  * initialiser the JVM resolves on first call.
  */
final class LazyGenericAvroSerde extends Serde[GenericRecord] {

  @volatile private var constructed               = false
  private lazy val delegate: Serde[GenericRecord] = {
    constructed = true
    ConfluentSerdes.newAvroSerde()
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = delegate.configure(configs, isKey)

  /** Closing a serde nobody used must not be the thing that drags Confluent onto the classpath. */
  override def close(): Unit = if (constructed) delegate.close()

  override def serializer(): Serializer[GenericRecord] = delegate.serializer()

  override def deserializer(): Deserializer[GenericRecord] = delegate.deserializer()
}
