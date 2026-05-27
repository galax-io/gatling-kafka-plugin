package org.galaxio.gatling.kafka.actions

private[actions] object KafkaRequestFailureMessages {
  def buildFailure(error: String): String =
    s"Failed to build request: $error"

  def sendFailure(error: String): String =
    s"Failed to send request to Kafka broker: $error"
}
