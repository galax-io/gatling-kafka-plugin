package org.galaxio.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;

import java.time.Duration;
import java.util.Map;

import static io.gatling.javaapi.core.CoreDsl.*;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.byteArrayExp;

public class OnlyConsumeSimulation extends Simulation {

    private final org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderNew protocol = KafkaDsl.kafka().requestReply()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                    )
            )
            .consumeSettings(Map.of("bootstrap.servers", "localhost:9092"))
            .timeout(Duration.ofSeconds(5));

    private final ScenarioBuilder scn = scenario("ConsumeOnly")
            .exec(session -> session.set("matchId", "corr-42".getBytes()))
            .exec(
                    KafkaDsl.kafka("Consume only")
                            .receiveFrom("events")
                            .matchIdForTracking(byteArrayExp(session -> (byte[]) session.get("matchId")))
                            .replyMatchBy(message -> message.value())
                            .saveAs("replyValue", message -> new String(message.value()))
            )
            .exec(
                    KafkaDsl.kafka("Consume any")
                            .consumeAny("events")
                            .saveAs("firstPayload", message -> new String(message.value()))
            );

    {
        setUp(scn.injectOpen(atOnceUsers(1))).protocols(protocol);
    }
}
