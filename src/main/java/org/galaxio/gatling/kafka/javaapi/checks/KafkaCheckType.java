package org.galaxio.gatling.kafka.javaapi.checks;

import io.gatling.javaapi.core.CheckBuilder.CheckType;

/**
 * Check types the Kafka DSL contributes on top of Gatling's own {@code CoreCheckType}s.
 *
 * <p>{@code ResponseCode} was removed in 2.0.0. Nothing ever produced a check builder carrying it, and its
 * materialization branch was byte-identical to {@code Simple}'s.
 */
public enum KafkaCheckType implements CheckType {
    Simple
}
