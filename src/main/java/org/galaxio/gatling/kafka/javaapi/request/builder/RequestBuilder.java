package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;

/**
 * Java-facing wrapper around a produce-only Kafka request builder.
 *
 * <p>Distinct from the Scala type it wraps, which is
 * {@code org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilder}. The single-implementation
 * {@code RequestBuilder} trait that used to sit between them was folded into that class in 2.0.0.
 */
public class RequestBuilder<K, V> implements ActionBuilder {

    private final org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilder<K, V> wrapped;

    public RequestBuilder(org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilder<K, V> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return wrapped.build();
    }
}
