package io.cosmospf.gatling.kafka.protocol

import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session
import io.cosmospf.gatling.kafka.client.{KafkaSender, TrackersPool}

case class KafkaComponents(
    coreComponents: CoreComponents,
    kafkaProtocol: KafkaProtocol,
    trackersPool: TrackersPool,
    sender: KafkaSender,
) extends ProtocolComponents {

  override def onStart: Session => Session = Session.Identity

  override def onExit: Session => Unit = ProtocolComponents.NoopOnExit
}
