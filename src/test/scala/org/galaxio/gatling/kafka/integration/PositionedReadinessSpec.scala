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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
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

  /** How long delivery is given after readiness completes. Generous because it is only ever spent on a genuine failure: the
    * correct implementation delivers within a poll interval, so a run that spends this budget has found something real.
    */
  private val DeliveryBudget = 30.seconds

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
    // Without this the client default (true) commits offsets under a fixed group id, and a second run — or a reused
    // container — would resolve from the commit rather than from `latest`, quietly removing the very premise this spec
    // rests on and passing against a broken fix.
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> "false",
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

      // Both the "has anything arrived" signal and the sequence number the assertion reads, deliberately the same
      // variable: its -1 sentinel is the "nothing yet" state. A separate arrived-flag published *ahead* of this value is
      // what made this spec flaky — the wait below exited on the flag, read the sequence number before the callback had
      // stored it, and reported a delivery timeout it had never actually waited out.
      val firstDelivered = new AtomicLong(-1L)
      val consumer       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap),
        Set.empty,
        record => {
          val sequence = new String(record.value()).toLong
          firstDelivered.compareAndSet(-1L, sequence)
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

        // Let the stream run on so there is something after S to deliver. The condition is the same variable the
        // assertion reads, so the loop cannot fall through to a value that has not been published yet.
        val waitingSince = System.currentTimeMillis()
        val deadline     = waitingSince + DeliveryBudget.toMillis
        while (firstDelivered.get() < 0 && System.currentTimeMillis() < deadline) Thread.sleep(20)
        val waited       = System.currentTimeMillis() - waitingSince

        val f = firstDelivered.get()
        // `waited` is reported rather than the budget: a timeout message that names a budget it never spent sends the
        // next reader after the broker instead of after the test.
        assert(
          f >= 0,
          s"nothing was delivered in the $DeliveryBudget after readiness completed; waited $waited ms " +
            s"(producer reached ${produced.get()})",
        )
        // `s + 1`, not `s`. `produced` is incremented before `sender.send`, and the send is asynchronous, so `S` counts
        // records handed to the producer while the consumer positions at the *appended* log end. When nothing happens to be
        // in flight at resolution time the correct implementation legitimately delivers `S + 1` first, and asserting `f <= s`
        // would fail a working fix over a one-record accounting artefact. Anything beyond that is a real skip.
        assert(
          f <= s + 1,
          s"the channel reported ready at record $s but the first record it delivered was $f: ${f - s - 1} records " +
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
