package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.ConfluentKafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaSender}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

/** Issue #193 — a reply channel must not report itself ready until it can actually receive.
  *
  * Readiness used to complete as soon as the topic appeared in `consumer.assignment()`. Assignment precedes fetch-position
  * resolution, and the plugin defaults `auto.offset.reset` to `latest`, so a record produced in that window is skipped: the
  * position resolves to the log end *after* it. The request then fails on its reply timeout, which reads in the report exactly
  * like a system under test that never answered.
  *
  * ==Why this measures an interval instead of racing one==
  *
  * The obvious test — publish one marker the instant readiness completes, assert it arrives — races the poll thread and can
  * pass by luck. Repeating it N times narrows the luck but never removes it, and a check that can pass by luck is not a gate.
  *
  * So this measures the gap rather than trying to land inside it. A producer emits a continuously numbered stream, started
  * before the subscription is requested and never paused:
  *
  *   - `S` = the sequence number the producer had reached when readiness completed. That is the "now" in the contract
  *     "everything published from now on will be seen".
  *   - `F` = the sequence number of the first record actually delivered.
  *
  * `F <= S` *is* that contract, stated directly. Before the fix the position resolves after readiness, so `F` lands past
  * everything produced in between and the difference is measurable — tens of records over milliseconds, not a knife edge. After
  * the fix the position is resolved before the future completes, so `F <= S` on every run.
  *
  * On failure the gap `F - S` is reported: that is the number of replies a real run would have silently skipped.
  */
class PositionedReadinessSpec extends munit.FunSuite with TestContainerForAll with StrictLogging {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  private val Topic = "positioned-readiness"

  /** Gap between records. Small enough that the assignment-to-position window spans many of them — with one record per second
    * the window would fit between two and the defect would be invisible — and large enough not to saturate the broker.
    */
  private val ProduceIntervalMillis = 2L

  /** Let the stream get going before requesting the subscription, so `latest` has somewhere to land: on an empty topic offset 0
    * is both the start and the end, and a skipped record cannot be told from a delivered one.
    */
  private val WarmUpRecords = 50

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  /** `latest` is the plugin's own default (`KafkaProtocolBuilder.withDefaultAutoReset`), and it is what makes the window
    * observable — with `earliest` a skipped record would be read anyway and the defect would hide.
    */
  private def consumerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
    ConsumerConfig.GROUP_ID_CONFIG                 -> s"$Topic-reader",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "latest",
  )

  private def createTopic(bootstrap: String): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap).asJava,
    )
    try admin.createTopics(List(new NewTopic(Topic, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
    finally admin.close()
  }

  test("a reply published the moment a channel reports ready is received") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      createTopic(bootstrap)

      val sender    = KafkaSender(producerSettings(bootstrap))
      val produced  = new AtomicLong(0L)
      val streaming = new AtomicBoolean(true)

      // Numbered, continuous, and never paused for the measurement. A stream that stopped while readiness
      // resolved would collapse S and F together and make the assertion vacuous.
      val producerThread = new Thread(
        () =>
          while (streaming.get) {
            val n = produced.incrementAndGet()
            sender.send(KafkaProtocolMessage(null, n.toString.getBytes, Topic, Topic))(
              _ => (),
              error => logger.error("[positioned-readiness] produce failed", error),
            )
            Thread.sleep(ProduceIntervalMillis)
          },
        "positioned-readiness-producer",
      )
      producerThread.setDaemon(true)
      producerThread.start()

      val firstDelivered = new AtomicLong(-1L)
      val delivered      = new AtomicInteger(0)
      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap),
        Set.empty,
        record => {
          delivered.incrementAndGet()
          firstDelivered.compareAndSet(-1L, new String(record.value()).toLong)
        },
        error => logger.error("[positioned-readiness] consumer failed", error),
      )
      val consumerThread = new Thread(consumer, "positioned-readiness-consumer")
      consumerThread.setDaemon(true)
      consumerThread.start()

      try {
        while (produced.get() < WarmUpRecords) Thread.sleep(10)

        val readiness = consumer.requestTopicSubscription(Topic)
        readiness.get(60, TimeUnit.SECONDS)

        // "Now", in the units the contract is written in. Sampled the instant readiness completed.
        val s = produced.get()

        // Let the stream run on so there is something after S to deliver.
        val deadline = System.currentTimeMillis() + 30000
        while (delivered.get() == 0 && System.currentTimeMillis() < deadline) Thread.sleep(20)

        val f = firstDelivered.get()
        assert(f >= 0, s"nothing was delivered within 30 s of readiness completing (producer reached ${produced.get()})")
        assert(
          f <= s,
          s"the channel reported ready at record $s but the first record it delivered was $f: ${f - s} records " +
            "published after readiness were skipped. Readiness completed on assignment alone, before the fetch " +
            "position was resolved, and `auto.offset.reset=latest` then positioned the consumer past them (issue #193)",
        )
      } finally {
        streaming.set(false)
        producerThread.join(5000)
        consumer.close()
        consumerThread.join(10000)
        sender.close()
      }
    }
  }
}
