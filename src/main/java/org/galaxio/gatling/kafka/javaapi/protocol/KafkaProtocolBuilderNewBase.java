package org.galaxio.gatling.kafka.javaapi.protocol;

import java.util.Map;

public class KafkaProtocolBuilderNewBase {

    public KPProducerSettingsStep producerSettings(Map<String, Object> ps) {
        return new KPProducerSettingsStep(ps);
    }

}

