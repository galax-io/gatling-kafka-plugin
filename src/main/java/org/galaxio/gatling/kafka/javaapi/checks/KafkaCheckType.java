package org.galaxio.gatling.kafka.javaapi.checks;

import io.gatling.javaapi.core.CheckBuilder.CheckType;

public enum KafkaCheckType implements CheckType {
    ResponseCode,
    Simple
}
