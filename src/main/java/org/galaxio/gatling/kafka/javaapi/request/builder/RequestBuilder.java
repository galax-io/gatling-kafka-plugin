package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;

import java.util.function.Function;

public class RequestBuilder<K, V> implements ActionBuilder {

    private final org.galaxio.gatling.kafka.request.builder.RequestBuilder<K, V> wrapped;

    public RequestBuilder(org.galaxio.gatling.kafka.request.builder.RequestBuilder<K,V> wrapped) {
        this.wrapped = wrapped;
    }

    public RequestBuilder<K, V> silent() {
        return make(org.galaxio.gatling.kafka.request.builder.RequestBuilder::silent);
    }

    public RequestBuilder<K, V> notSilent() {
        return make(org.galaxio.gatling.kafka.request.builder.RequestBuilder::notSilent);
    }

    private RequestBuilder<K, V> make(Function<
            org.galaxio.gatling.kafka.request.builder.RequestBuilder<K, V>,
            org.galaxio.gatling.kafka.request.builder.RequestBuilder<K, V>>
                                               f) {
        return new RequestBuilder<>(f.apply(wrapped));
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return wrapped.build();
    }
}
