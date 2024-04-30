package org.galaxio.gatling.kafka.actions

import com.softwaremill.quicklens._
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.builder.Avro4sAttributes

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

case class KafkaRequestAvro4sActionBuilder[K: ClassTag, V: ClassTag](attributes: Avro4sAttributes[K, V])
    extends ActionBuilder with NameGen {

  override def build(ctx: ScenarioContext, next: Action): Action = {
    import ctx._

    val kafkaComponents: KafkaProtocol.Components = protocolComponentsRegistry.components(KafkaProtocol.kafkaProtocolKey)

    val producer = new KafkaProducer[K, GenericRecord](kafkaComponents.kafkaProtocol.producerProperties.asJava)

    coreComponents.actorSystem.registerOnTermination(producer.close())

    new KafkaAvro4sRequestAction(
      producer,
      attributes,
      coreComponents,
      kafkaComponents.kafkaProtocol,
      throttled,
      next,
    )

  }
}
