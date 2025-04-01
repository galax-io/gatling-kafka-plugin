package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.commons.validation.Validation;
import io.gatling.core.session.Session;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.galaxio.gatling.kafka.javaapi.request.expressions.ExpressionBuilder;

import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.*;

import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression;
import scala.Function1;

import java.nio.ByteBuffer;

import static io.gatling.javaapi.core.internal.Expressions.*;
import static io.gatling.javaapi.core.internal.Expressions.toStaticValueExpression;

public class KafkaRequestBuilderBase {

    private final org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped;
    private final String requestName;

    private <T> Function1<Session, Validation<T>> calculateExpression(T obj) {
        if (obj == null)
            return null;
        if (obj instanceof String || obj.getClass().isPrimitive() || obj instanceof CharSequence) {
            return toExpression(obj.toString(), obj.getClass());
        }
        else {
            return toStaticValueExpression(obj);
        }
    }

    public KafkaRequestBuilderBase(org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped, String requestName) {
        this.wrapped = wrapped;
        this.requestName = requestName;
    }


    public RequestBuilder<String, String> send(String key, String payload) {
        return send(stringExp(cf(key)), stringExp(cf(payload)));
    }

    public RequestBuilder<String, String> send(String key, String payload, Headers headers) {
        return send(stringExp(cf(key)), stringExp(cf(payload)), headers);
    }

    public RequestBuilder<String, String> send(String key, String payload, JExpression<Headers> headers) {
        return send(stringExp(cf(key)), stringExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload) {
        return send(stringExp(cf(key)), payload);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload, Headers headers) {
        return send(stringExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(stringExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload) {
        return send(key, stringExp(cf(payload)));
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload, Headers headers) {
        return send(key, stringExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload, JExpression<Headers> headers) {
        return send(key, stringExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload) {
        return send(floatExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, Headers headers) {
        return send(floatExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(floatExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload) {
        return send(key, floatExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, Headers headers) {
        return send(key, floatExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, JExpression<Headers> headers) {
        return send(key, floatExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload) {
        return send(doubleExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, Headers headers) {
        return send(doubleExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(doubleExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload) {
        return send(key, doubleExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, Headers headers) {
        return send(key, doubleExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, JExpression<Headers> headers) {
        return send(key, doubleExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload) {
        return send(shortExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, Headers headers) {
        return send(shortExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(shortExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload) {
        return send(key, shortExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, Headers headers) {
        return send(key, shortExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, JExpression<Headers> headers) {
        return send(key, shortExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload) {
        return send(integerExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(integerExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(integerExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload) {
        return send(key, integerExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, Headers headers) {
        return send(key, integerExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, JExpression<Headers> headers) {
        return send(key, integerExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload) {
        return send(longExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, Headers headers) {
        return send(longExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(longExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload) {
        return send(key, longExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, Headers headers) {
        return send(key, longExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, JExpression<Headers> headers) {
        return send(key, longExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload) {
        return send(byteBufferExp(cf(key)), payload);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(byteBufferExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(byteBufferExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload) {
        return send(key, byteBufferExp(cf(payload)));
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, Headers headers) {
        return send(key, byteBufferExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, JExpression<Headers> headers) {
        return send(key, byteBufferExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload) {
        return send(bytesExp(cf(key)), payload);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, Headers headers) {
        return send(bytesExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(bytesExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload) {
        return send(key, bytesExp(cf(payload)));
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, Headers headers) {
        return send(key, bytesExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, JExpression<Headers> headers) {
        return send(key, bytesExp(cf(payload)), headers);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload) {
        return send(byteArrayExp(cf(key)), payload);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload, Headers headers) {
        return send(byteArrayExp(cf(key)), payload, headers);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(byteArrayExp(cf(key)), payload, headers);
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload) {
        return send(key, byteArrayExp(cf(payload)));
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload, Headers headers) {
        return send(key, byteArrayExp(cf(payload)), headers);
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload, JExpression<Headers> headers) {
        return send(key, byteArrayExp(cf(payload)), headers);
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload) {
        return send(key, payload, new RecordHeaders());
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload, Headers headers) {
        return new RequestBuilder<>(wrapped.send(
                key.gatlingExpression(),
                payload.gatlingExpression(),
                toStaticValueExpression(headers),
                org.galaxio.gatling.kafka.request.builder.Sender.noSchemaSender()
        ));
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload,
                                            JExpression<Headers> headers) {
        return new RequestBuilder<>(wrapped.send(
                key.gatlingExpression(),
                payload.gatlingExpression(),
                javaFunctionToExpression(headers),
                org.galaxio.gatling.kafka.request.builder.Sender.noSchemaSender()
        ));
    }

    public <V> RequestBuilder<Void, V> send(V payload) {
        return send(payload, new RecordHeaders());
    }

    public <V> RequestBuilder<Void, V> send(V payload, Headers headers) {
        return send(null, payload, headers);
    }

    public <V> RequestBuilder<Void, V> send(V payload, JExpression<Headers> headers) {
        return send(null, payload, headers);
    }

    public <K, V> RequestBuilder<K, V> send(K key, V payload) {
        return send(key, payload, new RecordHeaders());
    }

    public <K, V> RequestBuilder<K, V> send(K key, V payload, Headers headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        calculateExpression(key),
                        calculateExpression(payload),
                        toStaticValueExpression(headers),
                        org.galaxio.gatling.kafka.request.builder.Sender.noSchemaSender()
                ));
    }

    public <K, V> RequestBuilder<K, V> send(K key, V payload, JExpression<Headers> headers) {
        return new RequestBuilder<>(
                wrapped.send(
                        calculateExpression(key),
                        calculateExpression(payload),
                        javaFunctionToExpression(headers),
                        org.galaxio.gatling.kafka.request.builder.Sender.noSchemaSender()
                ));
    }

    public ReqRepBase requestReply() {
        return new ReqRepBase(requestName);
    }

}
