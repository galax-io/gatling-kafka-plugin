package io.cosmospf.gatling.kafka.javaapi;

import static io.gatling.javaapi.core.internal.Expressions.*;

import io.gatling.core.check.CheckBuilder;
import org.apache.avro.generic.GenericRecord;
import io.cosmospf.gatling.kafka.javaapi.checks.KafkaChecks;
import io.cosmospf.gatling.kafka.javaapi.protocol.*;
import io.cosmospf.gatling.kafka.javaapi.request.builder.*;
import io.cosmospf.gatling.kafka.request.KafkaProtocolMessage;
import scala.Function1;

public final class KafkaDsl {

    public static KafkaProtocolBuilderBase kafka() {
        return new KafkaProtocolBuilderBase();
    }

    public static KafkaRequestBuilderBase kafka(String requestName) {
        return new KafkaRequestBuilderBase(io.cosmospf.gatling.kafka.Predef.kafka(toStringExpression(requestName)), requestName);
    }

    public static KafkaChecks.KafkaCheckTypeWrapper simpleCheck(Function1<KafkaProtocolMessage, Boolean> f) {
        return new KafkaChecks.KafkaCheckTypeWrapper(new KafkaChecks.SimpleChecksScala().simpleCheck(f.andThen(Boolean::valueOf)));
    }

    public static CheckBuilder.Find<Object, KafkaProtocolMessage, GenericRecord> avroBody() {
        return new KafkaChecks.SimpleChecksScala().avroBody(io.cosmospf.gatling.kafka.javaapi.checks.KafkaChecks.avroSerde());
    }

}
