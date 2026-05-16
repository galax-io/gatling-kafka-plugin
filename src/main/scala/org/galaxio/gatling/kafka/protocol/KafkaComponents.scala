package org.galaxio.gatling.kafka.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.client.{KafkaSenderProvider, KafkaTrackerProviderFactory}

case class KafkaComponents(
    coreComponents: CoreComponents,
    kafkaProtocol: KafkaProtocol,
    trackersPoolFactory: KafkaTrackerProviderFactory,
    senderProvider: KafkaSenderProvider,
) extends ProtocolComponents {

  override def onStart: Session => Session = Session.Identity

  override def onExit: Session => Unit = ProtocolComponents.NoopOnExit
}
