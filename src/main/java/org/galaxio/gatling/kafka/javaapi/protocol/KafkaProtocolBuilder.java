package org.galaxio.gatling.kafka.javaapi.protocol;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ProtocolBuilder;

public class KafkaProtocolBuilder implements ProtocolBuilder {

    private final org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder wrapped;

    public KafkaProtocolBuilder(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Protocol protocol() {
        return wrapped.build();
    }

}
