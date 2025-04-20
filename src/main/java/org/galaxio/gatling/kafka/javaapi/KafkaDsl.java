package org.galaxio.gatling.kafka.javaapi;

import static io.gatling.javaapi.core.internal.Expressions.*;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.gatling.core.check.CheckBuilder;
import io.gatling.core.check.Check;
import io.gatling.core.check.CheckMaterializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.galaxio.gatling.kafka.javaapi.checks.KafkaCheckType;
import org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks;
import org.galaxio.gatling.kafka.javaapi.protocol.*;
import org.galaxio.gatling.kafka.javaapi.request.builder.*;
import org.galaxio.gatling.kafka.javaapi.request.expressions.Builders.*;
import org.galaxio.gatling.kafka.javaapi.request.expressions.ExpressionBuilder;
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression;
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage;

import java.nio.ByteBuffer;
import java.util.function.Function;

public final class KafkaDsl {

    public static <T> JExpression<T> cf(T t) {
        return i -> t;
    }

    public static ExpressionBuilder<String> stringExp(JExpression<String> f) {
        return new StringExpressionBuilder(f);
    }

    public static ExpressionBuilder<String> stringExp(String v) {
        return stringExp(cf(v));
    }

    public static ExpressionBuilder<Float> floatExp(JExpression<Float> f) {
        return new FloatExpressionBuilder(f);
    }

    public static ExpressionBuilder<Float> floatExp(Float v) {
        return floatExp(cf(v));
    }

    public static ExpressionBuilder<Double> doubleExp(JExpression<Double> f) {
        return new DoubleExpressionBuilder(f);
    }

    public static ExpressionBuilder<Double> doubleExp(Double v) {
        return doubleExp(cf(v));
    }

    public static ExpressionBuilder<Short> shortExp(JExpression<Short> f) {
        return new ShortExpressionBuilder(f);
    }

    public static ExpressionBuilder<Short> shortExp(Short v) {
        return shortExp(cf(v));
    }

    public static ExpressionBuilder<Integer> integerExp(JExpression<Integer> f) {
        return new IntegerExpressionBuilder(f);
    }

    public static ExpressionBuilder<Integer> integerExp(Integer v) {
        return integerExp(cf(v));
    }

    public static ExpressionBuilder<Long> longExp(JExpression<Long> f) {
        return new LongExpressionBuilder(f);
    }

    public static ExpressionBuilder<Long> longExp(Long v) {
        return longExp(cf(v));
    }

    public static ExpressionBuilder<ByteBuffer> byteBufferExp(JExpression<ByteBuffer> f) {
        return new ByteBufferExpressionBuilder(f);
    }

    public static ExpressionBuilder<ByteBuffer> byteBufferExp(ByteBuffer v) {
        return byteBufferExp(cf(v));
    }

    public static ExpressionBuilder<byte[]> byteArrayExp(byte[] v) {
        return byteArrayExp(cf(v));
    }

    public static ExpressionBuilder<byte[]> byteArrayExp(JExpression<byte[]> f) {
        return new ByteArrayExpressionBuilder(f);
    }

    public static ExpressionBuilder<Bytes> bytesExp(JExpression<Bytes> f) {
        return new BytesExpressionBuilder(f);
    }

    public static ExpressionBuilder<Bytes> bytesExp(Bytes v) {
        return bytesExp(cf(v));
    }

    public static AvroExpressionBuilder avro(Object o, SchemaRegistryClient client) {
        return avro(cf(o), client);
    }

    public static AvroExpressionBuilder avro(JExpression<Object> s, SchemaRegistryClient client) {
        return new AvroExpressionBuilder(s, client);
    }

    public static AvroExpressionBuilder avro(JExpression<Object> s, Serializer<Object> ser, Deserializer<Object> deser) {
        return new AvroExpressionBuilder(s, ser, deser);
    }

    public static KafkaProtocolBuilderBase kafka() {
        return new KafkaProtocolBuilderBase();
    }

    public static KafkaRequestBuilderBase kafka(String requestName) {
        return new KafkaRequestBuilderBase(org.galaxio.gatling.kafka.Predef.kafka(toStringExpression(requestName)), requestName);
    }


    public static io.gatling.javaapi.core.CheckBuilder simpleCheck(Function<KafkaProtocolMessage, Boolean> f) {
        return new io.gatling.javaapi.core.CheckBuilder() {
            @Override
            @SuppressWarnings("rawtypes")
            public CheckBuilder<?, ?> asScala() {
                return new CheckBuilder() {
                    @Override
                    public Check<?> build( CheckMaterializer materializer) {
                        return new KafkaChecks.SimpleChecksScala().simpleCheck(f::apply);

                    }
                };
            }

            @Override
            public CheckType type() {
                return KafkaCheckType.Simple;
            }
        };
    }

    public static CheckBuilder.Find<Object, KafkaProtocolMessage, GenericRecord> avroBody() {
        return new KafkaChecks.SimpleChecksScala().avroBody(org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks.avroSerde());
    }

}
