package org.galaxio.gatling.kafka.javaapi.request.builder;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.galaxio.gatling.kafka.javaapi.request.expressions.ExpressionBuilder;
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression;
import org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase;
import scala.reflect.ClassTag;

import java.nio.ByteBuffer;

import static io.gatling.javaapi.core.internal.Expressions.javaFunctionToExpression;
import static io.gatling.javaapi.core.internal.Expressions.toStaticValueExpression;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.*;

public class OnlyPublishStep {
    private final org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase.OnlyPublishStep wrapped;

    public OnlyPublishStep(KafkaRequestBuilderBase.OnlyPublishStep wrapped) {
        this.wrapped = wrapped;
    }

    public RequestBuilder<String, String> send(String key, String payload) {
        return send(stringExp(key), stringExp(payload));
    }

    public RequestBuilder<String, String> send(String key, String payload, Headers headers) {
        return send(stringExp(key), stringExp(payload), headers);
    }

    public RequestBuilder<String, String> send(String key, String payload, JExpression<Headers> headers) {
        return send(stringExp(key), stringExp(payload), headers);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload) {
        return send(stringExp(key), payload);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload, Headers headers) {
        return send(stringExp(key), payload, headers);
    }

    public <V> RequestBuilder<String, V> send(String key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(stringExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload) {
        return send(key, stringExp(payload));
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload, Headers headers) {
        return send(key, stringExp(payload), headers);
    }

    public <K> RequestBuilder<K, String> send(ExpressionBuilder<K> key, String payload, JExpression<Headers> headers) {
        return send(key, stringExp(payload), headers);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload) {
        return send(floatExp(key), payload);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, Headers headers) {
        return send(floatExp(key), payload, headers);
    }

    public <V> RequestBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(floatExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload) {
        return send(key, floatExp(payload));
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, Headers headers) {
        return send(key, floatExp(payload), headers);
    }

    public <K> RequestBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, JExpression<Headers> headers) {
        return send(key, floatExp(payload), headers);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload) {
        return send(doubleExp(key), payload);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, Headers headers) {
        return send(doubleExp(key), payload, headers);
    }

    public <V> RequestBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(doubleExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload) {
        return send(key, doubleExp(payload));
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, Headers headers) {
        return send(key, doubleExp(payload), headers);
    }

    public <K> RequestBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, JExpression<Headers> headers) {
        return send(key, doubleExp(payload), headers);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload) {
        return send(shortExp(key), payload);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, Headers headers) {
        return send(shortExp(key), payload, headers);
    }

    public <V> RequestBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(shortExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload) {
        return send(key, shortExp(payload));
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, Headers headers) {
        return send(key, shortExp(payload), headers);
    }

    public <K> RequestBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, JExpression<Headers> headers) {
        return send(key, shortExp(payload), headers);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload) {
        return send(integerExp(key), payload);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(integerExp(key), payload, headers);
    }

    public <V> RequestBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(integerExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload) {
        return send(key, integerExp(payload));
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, Headers headers) {
        return send(key, integerExp(payload), headers);
    }

    public <K> RequestBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, JExpression<Headers> headers) {
        return send(key, integerExp(payload), headers);
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload) {
        return send(longExp(key), payload);
    }

    public RequestBuilder<Integer, Long> send(Integer key, Long payload) {
        return send(integerExp(key), longExp(payload));
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, Headers headers) {
        return send(longExp(key), payload, headers);
    }

    public <V> RequestBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(longExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload) {
        return send(key, longExp(payload));
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, Headers headers) {
        return send(key, longExp(payload), headers);
    }

    public <K> RequestBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, JExpression<Headers> headers) {
        return send(key, longExp(payload), headers);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload) {
        return send(byteBufferExp(key), payload);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(byteBufferExp(key), payload, headers);
    }

    public <V> RequestBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(byteBufferExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload) {
        return send(key, byteBufferExp(payload));
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, Headers headers) {
        return send(key, byteBufferExp(payload), headers);
    }

    public <K> RequestBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, JExpression<Headers> headers) {
        return send(key, byteBufferExp(payload), headers);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload) {
        return send(bytesExp(key), payload);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, Headers headers) {
        return send(bytesExp(key), payload, headers);
    }

    public <V> RequestBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(bytesExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload) {
        return send(key, bytesExp(payload));
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, Headers headers) {
        return send(key, bytesExp(payload), headers);
    }

    public <K> RequestBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, JExpression<Headers> headers) {
        return send(key, bytesExp(payload), headers);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload) {
        return send(byteArrayExp(key), payload);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload, Headers headers) {
        return send(byteArrayExp(key), payload, headers);
    }

    public <V> RequestBuilder<byte[], V> send(byte[] key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(byteArrayExp(key), payload, headers);
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload) {
        return send(key, byteArrayExp(payload));
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload, Headers headers) {
        return send(key, byteArrayExp(payload), headers);
    }

    public <K> RequestBuilder<K, byte[]> send(ExpressionBuilder<K> key, byte[] payload, JExpression<Headers> headers) {
        return send(key, byteArrayExp(payload), headers);
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload) {
        return send(key, payload, new RecordHeaders());
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload, Headers headers) {
        return new RequestBuilder<K, V>(wrapped.send(
                key.gatlingExpression(),
                payload.gatlingExpression(),
                toStaticValueExpression(headers),
                key.getSerde(),
                ClassTag.apply(key.getType()),
                payload.getSerde(),
                ClassTag.apply(payload.getType())
        ));
    }

    public <K, V> RequestBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload,
                                            JExpression<Headers> headers) {
        return new RequestBuilder<>(wrapped.send(
                key.gatlingExpression(),
                payload.gatlingExpression(),
                javaFunctionToExpression(headers),
                key.getSerde(),
                ClassTag.apply(key.getType()),
                payload.getSerde(),
                ClassTag.apply(payload.getType())
        ));
    }

    @SuppressWarnings("unchecked")
    public <V> RequestBuilder<Object, V> sendWithClass(V payload, Class<V> vClass) {
        var res = (Object) wrapped.send(toStaticValueExpression(payload), Serdes.serdeFrom(vClass), ClassTag.apply(vClass));
        return new RequestBuilder<>(
                (org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilder<Object, V>)
                        res);
    }

    @SuppressWarnings("unchecked")
    public <V> RequestBuilder<Object, V> sendWithClass(V payload, Class<V> vClass, Headers headers) {
        var res = (Object) wrapped.send(
                null,
                toStaticValueExpression(payload),
                toStaticValueExpression(headers),
                Serdes.serdeFrom(String.class),
                ClassTag.apply(String.class),
                Serdes.serdeFrom(vClass),
                ClassTag.apply(vClass));
        return new RequestBuilder<>(
                (org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilder<Object, V>)
                        res);
    }


    public RequestBuilder<Object, String> send(String string) {
        return sendWithClass(string, String.class);
    }

    public RequestBuilder<Object, String> send(String string, Headers headers) {
        return sendWithClass(string, String.class, headers);
    }

    public RequestBuilder<Object, Integer> send(Integer value) {
        return sendWithClass(value, Integer.class);
    }

    public RequestBuilder<Object, Long> send(Long value) {
        return sendWithClass(value, Long.class);
    }

    public RequestBuilder<String, Double> send(String string, double v) {
        return send(stringExp(string), doubleExp(v));
    }


}
