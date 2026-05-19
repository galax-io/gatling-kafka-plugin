package org.galaxio.gatling.kafka.javaapi.request.expressions;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

public class Builders {
    public Builders() {
    }

    public static final class FloatExpressionBuilder extends ExpressionBuilder<Float> {
        public FloatExpressionBuilder(JExpression<Float> javaExpression) {
            super(javaExpression, Float.class, Serdes.Float());
        }
    }

    public static final class DoubleExpressionBuilder extends ExpressionBuilder<Double> {
        public DoubleExpressionBuilder(JExpression<Double> javaExpression) {
            super(javaExpression, Double.class, Serdes.Double());
        }
    }

    public static final class ShortExpressionBuilder extends ExpressionBuilder<Short> {
        public ShortExpressionBuilder(JExpression<Short> javaExpression) {
            super(javaExpression, Short.class, Serdes.Short());
        }
    }

    public static final class IntegerExpressionBuilder extends ExpressionBuilder<Integer> {
        public IntegerExpressionBuilder(JExpression<Integer> javaExpression) {
            super(javaExpression, Integer.class, Serdes.Integer());
        }
    }

    public static final class LongExpressionBuilder extends ExpressionBuilder<Long> {
        public LongExpressionBuilder(JExpression<Long> javaExpression) {
            super(javaExpression, Long.class, Serdes.Long());
        }
    }

    public static final class ByteArrayExpressionBuilder extends ExpressionBuilder<byte[]> {
        public ByteArrayExpressionBuilder(JExpression<byte[]> javaExpression) {
            super(javaExpression, byte[].class, Serdes.ByteArray());
        }
    }

    public static final class ByteBufferExpressionBuilder extends ExpressionBuilder<ByteBuffer> {
        public ByteBufferExpressionBuilder(JExpression<ByteBuffer> javaExpression) {
            super(javaExpression, ByteBuffer.class, Serdes.ByteBuffer());
        }
    }

    public static final class BytesExpressionBuilder extends ExpressionBuilder<Bytes> {
        public BytesExpressionBuilder(JExpression<Bytes> javaExpression) {
            super(javaExpression, Bytes.class, Serdes.Bytes());
        }
    }

    public static final class StringExpressionBuilder extends ExpressionBuilder<String> {
        public StringExpressionBuilder(JExpression<String> javaExpression) {
            super(javaExpression, String.class, Serdes.String());
        }
    }

    public static final class AvroExpressionBuilder extends ExpressionBuilder<Object> {
        public AvroExpressionBuilder(JExpression<Object> valueF, SchemaRegistryClient client) {
            super(valueF, Object.class, Serdes.serdeFrom(new KafkaAvroSerializer(client), new KafkaAvroDeserializer(client)));
        }

        public AvroExpressionBuilder(JExpression<Object> valueF, Serializer<Object> ser, Deserializer<Object> deser) {
            super(valueF, Object.class, Serdes.serdeFrom(ser, deser));
        }
    }

}
