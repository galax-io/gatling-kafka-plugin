package org.galaxio.gatling.kafka.javaapi.examples

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.javaapi.core.CoreDsl.*
import io.gatling.javaapi.core.Session
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.*
import java.nio.charset.Charset
import java.time.Duration

class ProducerSimulation : Simulation() {

    // example of using a custom serde: avro(...) takes the serializer/deserializer pair directly,
    // so the Schema Registry client is yours to configure rather than the plugin's.
    private val ser = KafkaAvroSerializer(CachedSchemaRegistryClient("http://localhost:9094".split(','), 16))
        as Serializer<Any>
    private val de = KafkaAvroDeserializer(CachedSchemaRegistryClient("http://localhost:9094".split(','), 16))
        as Deserializer<Any>

    private val kafkaConsumerConf = kafka()
        .properties(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
            )
        )

    private fun header(session: Session): Headers =
        RecordHeaders().add("uuid-header", (session.getString("UUID") ?: "").toByteArray(Charset.defaultCharset()))

    private val scn = scenario("Basic")
        .exec(kafka("BasicRequest").topic("test.topic").send("foo"))
        .exec(kafka("dld").topic("test.topic").send("true", 12.0))
        .exec(kafka("Msg1").topic("test.topic").send("key", "val", this::header))
        .exec(
            kafka("avro_serde").topic("test.topic")
                .send(stringExp("#{sessionIdKey}"), avro({ session: Session -> session.get("event") }, ser, de))
        )

    init {
        setUp(
            scn.injectOpen(atOnceUsers(1))
        )
            .protocols(kafkaConsumerConf)
            .maxDuration(Duration.ofSeconds(120))
    }
}
