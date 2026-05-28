package org.galaxio.gatling.kafka.actions

private[actions] object KafkaRequestFailureMessages {
  def buildFailure(error: String): String =
    s"Failed to build request: ${Option(error).getOrElse("unknown error")}"

  def sendFailure(error: String): String =
    s"Failed to send request to Kafka broker: ${Option(error).getOrElse("unknown error")}"

  def sendFailure(exception: Throwable): String =
    sendFailure(Option(exception.getMessage).getOrElse(exception.getClass.getSimpleName))
}
