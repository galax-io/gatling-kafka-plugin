package org.galaxio.gatling.kafka.javaapi.examples;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.gatling.javaapi.core.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderNew;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.*;
import org.apache.kafka.common.serialization.*;
import org.galaxio.gatling.kafka.javaapi.request.builder.RequestReplyBuilder;

import java.time.Duration;
import java.util.*;

import static io.gatling.javaapi.core.CoreDsl.*;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.*;

public class AvroClassWithRequestReplySimulation extends Simulation {
    private static final SchemaRegistryClient client = new CachedSchemaRegistryClient(Arrays.asList("schRegUrl".split(",")), 16);

    // example of using custom serde
    public static Serializer<MyAvroClass> ser =
            (Serializer) new KafkaAvroSerializer(client);
    public static Deserializer<MyAvroClass> de =
            (Deserializer) new KafkaAvroDeserializer(client);

    // protocol
    private final KafkaProtocolBuilderNew kafkaProtocolRRAvro = kafka().requestReply()
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

    private static class MyAvroClass {
    }
}
