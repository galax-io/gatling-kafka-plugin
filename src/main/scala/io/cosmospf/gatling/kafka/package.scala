package io.cosmospf.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.check.Check
import io.cosmospf.gatling.kafka.request.KafkaProtocolMessage

package object kafka {
  type KafkaCheck = Check[KafkaProtocolMessage]

  trait KafkaLogging extends StrictLogging {
    def logMessage(text: => String, msg: KafkaProtocolMessage): Unit = {
      logger.debug(text)
      logger.trace(msg.toString)
    }
  }
}
