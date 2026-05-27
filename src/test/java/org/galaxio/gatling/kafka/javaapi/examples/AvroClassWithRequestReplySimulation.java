package org.galaxio.gatling.kafka.javaapi.examples;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import org.galaxio.gatling.kafka.javaapi.request.builder.RequestReplyBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class AvroClassWithRequestReplySimulation extends Simulation {
    private static final SchemaRegistryClient client = new CachedSchemaRegistryClient(Arrays.asList("schRegUrl".split(",")), 16);

    // example of using custom serde
    public static Serializer<MyAvroClass> ser = serializer(client);
    public static Deserializer<MyAvroClass> de = deserializer(client);

    // protocol
    private final KafkaProtocolBuilder kafkaProtocolRRAvro = kafka()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093",
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                            // schema registry url is required for KafkaAvroSerializer and KafkaAvroDeserializer
                            "schema.registry.url", "http://localhost:9094"
                    )
            )
            .consumeSettings(
                    Map.of("bootstrap.servers", "localhost:9093")
            )
            .timeout(Duration.ofSeconds(5));

    // message
    public static RequestReplyBuilder<?, ?> kafkaMessage = kafka("RequestReply").requestReply()
            .requestTopic("request.t")
            .replyTopic("reply.t")
            .send("key", new MyAvroClass(), String.class, MyAvroClass.class, ser, de);

    // simulation
    {
        setUp(scenario("Kafka RequestReply Avro").exec(kafkaMessage).injectOpen(atOnceUsers(1))).protocols(kafkaProtocolRRAvro);
    }

    private static Serializer<MyAvroClass> serializer(SchemaRegistryClient client) {
        KafkaAvroSerializer delegate = new KafkaAvroSerializer(client);
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, MyAvroClass data) {
                return delegate.serialize(topic, data);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private static Deserializer<MyAvroClass> deserializer(SchemaRegistryClient client) {
        KafkaAvroDeserializer delegate = new KafkaAvroDeserializer(client);
        return new Deserializer<>() {
            @Override
            public MyAvroClass deserialize(String topic, byte[] data) {
                Object value = delegate.deserialize(topic, data);
                return value == null ? null : MyAvroClass.class.cast(value);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private static class MyAvroClass {
    }
}
