package org.galaxio.examples.javaapi;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.gatling.javaapi.core.Simulation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import org.galaxio.gatling.kafka.javaapi.request.builder.RequestReplyBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class AvroClassWithRequestReplySimulation extends Simulation {

    private static final String SCHEMA_REGISTRY_URL = "http://localhost:9094";

    private static final SchemaRegistryClient client =
            new CachedSchemaRegistryClient(Arrays.asList(SCHEMA_REGISTRY_URL.split(",")), 16);

    // The payload must be something Avro can serialize: a record, or one of the primitives Avro
    // supports. A plain Java class is not one of those, and KafkaAvroSerializer rejects it before a
    // request is ever sent.
    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"Ingredient\","
                    + "\"namespace\":\"org.galaxio.gatling.kafka.javaapi.examples\","
                    + "\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"sugar\",\"type\":\"double\"},"
                    + "{\"name\":\"fat\",\"type\":\"double\"}"
                    + "]}");

    private static GenericRecord ingredient() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("name", "Cheese");
        record.put("sugar", 0.0d);
        record.put("fat", 70.0d);
        return record;
    }

    // Pass the config as well as the client: constructed with a client alone, KafkaAvroSerializer never
    // registers a schema, it only looks one up — and fails with "Error retrieving Avro schema" the first
    // time it meets a type the registry has not seen.
    private static final Map<String, Object> SERDE_CONFIG =
            Map.of("schema.registry.url", SCHEMA_REGISTRY_URL, "auto.register.schemas", true);

    // example of using custom serde
    public static Serializer<GenericRecord> ser = serializer(client);
    public static Deserializer<GenericRecord> de = deserializer(client);

    // protocol
    private final KafkaProtocolBuilder kafkaProtocolRRAvro = kafka()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093",
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                            // schema registry url is required for KafkaAvroSerializer and KafkaAvroDeserializer
                            "schema.registry.url", SCHEMA_REGISTRY_URL
                    )
            )
            .consumeSettings(
                    Map.of("bootstrap.servers", "localhost:9093")
            )
            .timeout(Duration.ofSeconds(10));

    // message
    public static RequestReplyBuilder<?, ?> kafkaMessage = kafka("RequestReply").requestReply()
            .requestTopic("ex.java.avrorr.t")
            .replyTopic("ex.java.avrorr.t")
            .send("key", ingredient(), String.class, GenericRecord.class, ser, de);

    // simulation
    {
        setUp(scenario("Kafka RequestReply Avro").exec(kafkaMessage).injectOpen(atOnceUsers(1)))
                .protocols(kafkaProtocolRRAvro)
                .assertions(
                        global().allRequests().count().is(1L),
                        global().successfulRequests().percent().is(100.0)
                );
    }

    private static Serializer<GenericRecord> serializer(SchemaRegistryClient client) {
        KafkaAvroSerializer delegate = new KafkaAvroSerializer(client, SERDE_CONFIG);
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, GenericRecord data) {
                return delegate.serialize(topic, data);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private static Deserializer<GenericRecord> deserializer(SchemaRegistryClient client) {
        KafkaAvroDeserializer delegate = new KafkaAvroDeserializer(client, SERDE_CONFIG);
        return new Deserializer<>() {
            @Override
            public GenericRecord deserialize(String topic, byte[] data) {
                Object value = delegate.deserialize(topic, data);
                return value == null ? null : GenericRecord.class.cast(value);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
