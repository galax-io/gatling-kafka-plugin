package org.galaxio.gatling.kafka.javaapi;

import static io.gatling.javaapi.core.internal.Expressions.*;

import io.gatling.core.check.CheckBuilder;
import org.apache.avro.generic.GenericRecord;
import org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks;
import org.galaxio.gatling.kafka.javaapi.protocol.*;
import org.galaxio.gatling.kafka.javaapi.request.builder.*;
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderBase;
import org.galaxio.gatling.kafka.javaapi.request.builder.KafkaRequestBuilderBase;
import scala.Function1;

public final class KafkaDsl {

    public static KafkaProtocolBuilderBase kafka() {
        return new KafkaProtocolBuilderBase();
    }

    public static KafkaRequestBuilderBase kafka(String requestName) {
        return new KafkaRequestBuilderBase(org.galaxio.gatling.kafka.Predef.kafka(toStringExpression(requestName)), requestName);
    }

    public static KafkaChecks.KafkaCheckTypeWrapper simpleCheck(Function1<KafkaProtocolMessage, Boolean> f) {
        return new KafkaChecks.KafkaCheckTypeWrapper(new KafkaChecks.SimpleChecksScala().simpleCheck(f.andThen(Boolean::valueOf)));
    }

    public static CheckBuilder.Find<Object, KafkaProtocolMessage, GenericRecord> avroBody() {
        return new KafkaChecks.SimpleChecksScala().avroBody(org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks.avroSerde());
    }

}
