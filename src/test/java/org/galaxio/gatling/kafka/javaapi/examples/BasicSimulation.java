package org.galaxio.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;

public class BasicSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaConf = KafkaDsl.kafka()
            .properties(Map.of(ProducerConfig.ACKS_CONFIG, "1"));

    private final KafkaProtocolBuilder kafkaProtocolC = KafkaDsl.kafka()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                    )
            )
            .consumeSettings(
                    Map.of("bootstrap.servers", "localhost:9092")
            ).timeout(Duration.ofSeconds(5));

    private final AtomicInteger c = new AtomicInteger(0);

    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("kekey", c.incrementAndGet())
            ).iterator();

    private final Headers headers = new RecordHeaders().add("test-header", "test_value".getBytes());

    private final ScenarioBuilder scn = scenario("Basic")
            .feed(feeder)
            .exec(
                    KafkaDsl.kafka("ReqRep").requestReply()
                            .requestTopic("test.t")
                            .replyTopic("test.t")
                            .send("#{kekey}", """
                                    { "m": "dkf" }
                                    """, headers, String.class, String.class)
                            .check(jsonPath("$.m").is("dkf"))
            );

    {
        setUp(scn.injectOpen(atOnceUsers(5))).protocols(kafkaProtocolC).maxDuration(Duration.ofSeconds(120));
    }

}
