package org.galaxio.gatling.kafka.client

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

trait KafkaSenderProvider {
  def sender(producerSettings: Map[String, AnyRef]): KafkaSender
  def close(): Unit
}

class KafkaSenderPool extends KafkaSenderProvider {
  private val senders = new ConcurrentHashMap[Map[String, AnyRef], KafkaSender]()

  override def sender(producerSettings: Map[String, AnyRef]): KafkaSender =
    senders.computeIfAbsent(
      producerSettings,
      KafkaSender(_),
    )

  override def close(): Unit =
    senders.values().asScala.foreach(_.close())
}
