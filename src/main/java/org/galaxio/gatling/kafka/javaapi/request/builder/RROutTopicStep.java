package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.commons.validation.Validation;
import io.gatling.core.session.Session;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.galaxio.gatling.kafka.javaapi.request.expressions.ExpressionBuilder;
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression;
import org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase;
import scala.reflect.ClassTag;

import static io.gatling.javaapi.core.internal.Expressions.*;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.*;

import java.nio.ByteBuffer;

public class RROutTopicStep {

    private final scala.Function1<Session, Validation<String>> inputTopic;
    private final scala.Function1<Session, Validation<String>> outputTopic;
    private final String requestName;

    public RROutTopicStep(scala.Function1<Session, Validation<String>> inputTopic, scala.Function1<Session, Validation<String>> outputTopic, String requestName) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.requestName = requestName;
    }

    public <V> RequestReplyBuilder<String, V> send(String key, ExpressionBuilder<V> payload) {
        return send(stringExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<String, V> send(String key, ExpressionBuilder<V> payload, Headers headers) {
        return send(stringExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<String, V> send(String key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(stringExp(cf(key)), payload, headers);
    }

    public RequestReplyBuilder<String, String> send(String key, String payload) {
        return send(key, payload, new RecordHeaders());
    }

    public RequestReplyBuilder<String, String> send(String key, String payload, Headers headers) {
        return send(stringExp(cf(key)), stringExp(cf(payload)), headers);
    }

    public RequestReplyBuilder<String, String> send(String key, String payload, JExpression<Headers> headers) {
        return send(stringExp(cf(key)), stringExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, String> send(ExpressionBuilder<K> key, String payload) {
        return send(key, stringExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, String> send(ExpressionBuilder<K> key, String payload, Headers headers) {
        return send(key, stringExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, String> send(ExpressionBuilder<K> key, String payload, JExpression<Headers> headers) {
        return send(key, stringExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload) {
        return send(floatExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, Headers headers) {
        return send(floatExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Float, V> send(Float key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(floatExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload) {
        return send(key, floatExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, Headers headers) {
        return send(key, floatExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Float> send(ExpressionBuilder<K> key, Float payload, JExpression<Headers> headers) {
        return send(key, floatExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload) {
        return send(doubleExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, Headers headers) {
        return send(doubleExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Double, V> send(Double key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(doubleExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload) {
        return send(key, doubleExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, Headers headers) {
        return send(key, doubleExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Double> send(ExpressionBuilder<K> key, Double payload, JExpression<Headers> headers) {
        return send(key, doubleExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload) {
        return send(shortExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, Headers headers) {
        return send(shortExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Short, V> send(Short key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(shortExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload) {
        return send(key, shortExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, Headers headers) {
        return send(key, shortExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Short> send(ExpressionBuilder<K> key, Short payload, JExpression<Headers> headers) {
        return send(key, shortExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload) {
        return send(integerExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(integerExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Integer, V> send(Integer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(integerExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload) {
        return send(key, integerExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, Headers headers) {
        return send(key, integerExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Integer> send(ExpressionBuilder<K> key, Integer payload, JExpression<Headers> headers) {
        return send(key, integerExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload) {
        return send(longExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, Headers headers) {
        return send(longExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Long, V> send(Long key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(longExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload) {
        return send(key, longExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, Headers headers) {
        return send(key, longExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Long> send(ExpressionBuilder<K> key, Long payload, JExpression<Headers> headers) {
        return send(key, longExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload) {
        return send(byteBufferExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, Headers headers) {
        return send(byteBufferExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<ByteBuffer, V> send(ByteBuffer key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(byteBufferExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload) {
        return send(key, byteBufferExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, Headers headers) {
        return send(key, byteBufferExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, ByteBuffer> send(ExpressionBuilder<K> key, ByteBuffer payload, JExpression<Headers> headers) {
        return send(key, byteBufferExp(cf(payload)), headers);
    }

    public <V> RequestReplyBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload) {
        return send(bytesExp(cf(key)), payload);
    }

    public <V> RequestReplyBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, Headers headers) {
        return send(bytesExp(cf(key)), payload, headers);
    }

    public <V> RequestReplyBuilder<Bytes, V> send(Bytes key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return send(bytesExp(cf(key)), payload, headers);
    }

    public <K> RequestReplyBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload) {
        return send(key, bytesExp(cf(payload)));
    }

    public <K> RequestReplyBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, Headers headers) {
        return send(key, bytesExp(cf(payload)), headers);
    }

    public <K> RequestReplyBuilder<K, Bytes> send(ExpressionBuilder<K> key, Bytes payload, JExpression<Headers> headers) {
        return send(key, bytesExp(cf(payload)), headers);
    }

    public <K, V> RequestReplyBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload) {
        return send(key, payload, new RecordHeaders());
    }

    public <K, V> RequestReplyBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload, Headers headers) {
        return new RequestReplyBuilder<>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        key.gatlingExpression(),
                        payload.gatlingExpression(),
                        toStaticValueExpression(headers),
                        key.getSerde(),
                        ClassTag.apply(key.getType()),
                        payload.getSerde(),
                        ClassTag.apply(payload.getType())
                ));
    }

    public <K, V> RequestReplyBuilder<K, V> send(ExpressionBuilder<K> key, ExpressionBuilder<V> payload, JExpression<Headers> headers) {
        return new RequestReplyBuilder<>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        key.gatlingExpression(),
                        payload.gatlingExpression(),
                        javaFunctionToExpression(headers),
                        key.getSerde(),
                        ClassTag.apply(key.getType()),
                        payload.getSerde(),
                        ClassTag.apply(payload.getType())
                ));
    }

    public <K, V> RequestReplyBuilder<K, V> send(K key, V payload, Class<K> keyClass, Class<V> payloadClass) {
        return new RequestReplyBuilder<>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        toStaticValueExpression(new RecordHeaders()),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(payloadClass),
                        ClassTag.apply(payloadClass)
                ));
    }

    public <K, V> RequestReplyBuilder<K, V> send(K key, V payload, Headers headers, Class<K> keyClass, Class<V> payloadClass) {
        return new RequestReplyBuilder<>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        toStaticValueExpression(headers),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(payloadClass),
                        ClassTag.apply(payloadClass)
                ));
    }

    public <K, V> RequestReplyBuilder<K, V> send(K key, V payload, JExpression<Headers> headers, Class<K> keyClass, Class<V> payloadClass) {
        return new RequestReplyBuilder<>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        javaFunctionToExpression(headers),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(payloadClass),
                        ClassTag.apply(payloadClass)
                ));
    }

    public <K, V> RequestReplyBuilder<?, ?> send(K key, V payload, Class<K> keyClass, Class<V> payloadClass, Serializer<V> ser, Deserializer<V> de) {
        return new RequestReplyBuilder<K, V>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        toStaticValueExpression(new RecordHeaders()),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(ser, de),
                        ClassTag.apply(payloadClass)
                ));
    }

    public <K, V> RequestReplyBuilder<?, ?> send(K key, V payload, Headers headers, Class<K> keyClass, Class<V> payloadClass, Serializer<V> ser, Deserializer<V> de) {
        return new RequestReplyBuilder<K, V>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        toStaticValueExpression(headers),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(ser, de),
                        ClassTag.apply(payloadClass)
                ));
    }

    public <K, V> RequestReplyBuilder<?, ?> send(K key, V payload, JExpression<Headers> headers, Class<K> keyClass, Class<V> payloadClass, Serializer<V> ser, Deserializer<V> de) {
        return new RequestReplyBuilder<K, V>(KafkaRequestBuilderBase.apply(toStringExpression(this.requestName)).requestReply()
                .requestTopic(this.inputTopic)
                .replyTopic(this.outputTopic)
                .send(
                        toStaticValueExpression(key),
                        toStaticValueExpression(payload),
                        javaFunctionToExpression(headers),
                        Serdes.serdeFrom(keyClass),
                        ClassTag.apply(keyClass),
                        Serdes.serdeFrom(ser, de),
                        ClassTag.apply(payloadClass)
                ));
    }

}
