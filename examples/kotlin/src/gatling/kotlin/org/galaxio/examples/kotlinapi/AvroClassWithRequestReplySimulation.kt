package org.galaxio.examples.kotlinapi

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.javaapi.core.CoreDsl.atOnceUsers
import io.gatling.javaapi.core.CoreDsl.global
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka
import java.time.Duration

class AvroClassWithRequestReplySimulation : Simulation() {

    private val schemaRegistryUrl = "http://localhost:9094"

    private val client: SchemaRegistryClient = CachedSchemaRegistryClient(schemaRegistryUrl.split(","), 16)

    private val serdeConfig = mapOf<String, Any>(
        "schema.registry.url" to schemaRegistryUrl,
        "auto.register.schemas" to true,
    )

    private val schema: Schema = Schema.Parser().parse(
        """
        {"type":"record","name":"Ingredient","namespace":"org.galaxio.examples.kotlinapi",
         "fields":[{"name":"name","type":"string"},
                   {"name":"sugar","type":"double"},
                   {"name":"fat","type":"double"}]}
        """.trimIndent()
    )

    private fun ingredient(): GenericRecord {
        val record = GenericData.Record(schema)
        record.put("name", "Cheese")
        record.put("sugar", 0.0)
        record.put("fat", 70.0)
        return record
    }

    private val ser: Serializer<GenericRecord> = object : Serializer<GenericRecord> {
        private val delegate = KafkaAvroSerializer(client, serdeConfig)
        override fun serialize(topic: String, data: GenericRecord): ByteArray = delegate.serialize(topic, data)
        override fun close() = delegate.close()
    }

    private val de: Deserializer<GenericRecord> = object : Deserializer<GenericRecord> {
        private val delegate = KafkaAvroDeserializer(client, serdeConfig)
        override fun deserialize(topic: String, data: ByteArray?): GenericRecord? =
            delegate.deserialize(topic, data) as GenericRecord?
        override fun close() = delegate.close()
    }

    private val kafkaProtocolRRAvro = kafka()
        .producerSettings(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "io.confluent.kafka.serializers.KafkaAvroSerializer",
                "schema.registry.url" to schemaRegistryUrl,
            )
        )
        .consumeSettings(mapOf<String, Any>("bootstrap.servers" to "localhost:9093"))
        .timeout(Duration.ofSeconds(10))

    private val kafkaMessage = kafka("RequestReply").requestReply()
        .requestTopic("ex.kotlin.avrorr.t")
        .replyTopic("ex.kotlin.avrorr.t")
        .send("key", ingredient(), String::class.java, GenericRecord::class.java, ser, de)

    init {
        setUp(scenario("Kafka RequestReply Avro").exec(kafkaMessage).injectOpen(atOnceUsers(1)))
            .protocols(kafkaProtocolRRAvro)
            .assertions(
                global().allRequests().count().`is`(1L),
                global().successfulRequests().percent().`is`(100.0),
            )
    }
}
