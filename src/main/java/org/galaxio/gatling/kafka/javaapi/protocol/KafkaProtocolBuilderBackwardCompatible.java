package org.galaxio.gatling.kafka.javaapi.protocol;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ProtocolBuilder;

public class KafkaProtocolBuilderBackwardCompatible implements ProtocolBuilder {

    private final org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilderBackwardCompatible wrapped;

    public KafkaProtocolBuilderBackwardCompatible(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilderBackwardCompatible wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Protocol protocol() {
        return wrapped.build();
    }

}
