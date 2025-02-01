package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.commons.validation.Validation;
import io.gatling.core.session.Session;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.galaxio.gatling.kafka.request.builder.Sender;
import scala.Function1;

import static io.gatling.javaapi.core.internal.Expressions.*;
import static io.gatling.javaapi.core.internal.Expressions.toStaticValueExpression;

public class KafkaRequestBuilderBase {

    private final org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped;
    private final String requestName;

    private <T> Function1<Session, Validation<T>> calculateExpression(T obj) {
        Function1<Session, Validation<T>> expression;

        if (obj instanceof String || obj.getClass().isPrimitive() || obj instanceof CharSequence || obj instanceof byte[]) {
            expression = toExpression(obj.toString(), obj.getClass());
        } else {
            expression = toStaticValueExpression(obj);
        }
        return expression;
    }

    public KafkaRequestBuilderBase(org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped, String requestName) {
        this.wrapped = wrapped;
        this.requestName = requestName;
    }

    public <K, V> RequestBuilder<?, ?> send(K key, V payload) {
        return new RequestBuilder<>(
                wrapped.send(
                        calculateExpression(key),
                        calculateExpression(payload),
                        new RecordHeaders(),
                        Sender.noSchemaSender()
                ));
    }

    public <K, V> RequestBuilder<?, ?> send(K key, V payload, String headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        calculateExpression(key),
                        calculateExpression(payload),
                        toStaticValueExpression(headers),
                        Sender.noSchemaSender()
                ));
    }

    public <K, V> RequestBuilder<?, ?> send(K key, V payload, Headers headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        calculateExpression(key),
                        calculateExpression(payload),
                        headers,
                        Sender.noSchemaSender()
                ));
    }


    public <V> RequestBuilder<?, ?> send(V payload) {
        return new RequestBuilder<>(wrapped.send(
                calculateExpression(payload),
                Sender.noSchemaSender()));
    }

    public <K, V> RequestBuilder<?, ?> send(V payload, Headers headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        null,
                        calculateExpression(payload),
                        headers,
                        Sender.noSchemaSender()
                ));
    }

    public <K, V> RequestBuilder<?, ?> send(V payload, String headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        null,
                        calculateExpression(payload),
                        toStaticValueExpression(headers),
                        Sender.noSchemaSender()
                ));
    }

    public ReqRepBase requestReply() {
        return new ReqRepBase(requestName);
    }

}
