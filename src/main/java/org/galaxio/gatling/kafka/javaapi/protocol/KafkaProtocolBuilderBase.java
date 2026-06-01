package org.galaxio.gatling.kafka.javaapi.protocol;


import scala.jdk.javaapi.DurationConverters;

import java.time.Duration;
import java.util.Map;

import static scala.jdk.javaapi.CollectionConverters.asScala;

public class KafkaProtocolBuilderBase {

    public KPProducerSettingsStep producerSettings(Map<String, Object> ps) {
        return new KPProducerSettingsStep(ps);
    }

    public KafkaProtocolBuilder properties(Map<String, Object> producerSettings) {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(Map.of()));
        return new KafkaProtocolBuilder(
                org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder.apply(ps, cs, DurationConverters.toScala(Duration.ofSeconds(60)), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$)
        );
    }

}
