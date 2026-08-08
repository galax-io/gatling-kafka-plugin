package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.ConfluentKafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats.{OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.actor.ActorSystem
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.{LoggedResponse, RecordingStatsEngine}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Serdes}
import org.galaxio.gatling.kafka.actions.{KafkaRequestAction, KafkaRequestReplyAction}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMatcher, KafkaValueMatcher}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Issue #167 — a request-reply that carries no key must not share a correlation slot with every other keyless request.
  *
  * `KafkaAction` used to substitute `Array.emptyByteArray` for an absent key, and the tracker folded a null match id into that
  * same empty array. Every keyless request-reply therefore registered under one key in a plain `HashMap`: the second request
  * displaced the first, and the first reply to arrive resolved whichever request currently occupied the slot. One virtual user
  * was marked OK on someone else's reply while the real owner timed out.
  *
  * Two tests, and they are not the same kind of check:
  *
  *   - `keyless under key matching` is the regression gate. Before the fix these requests were registered and sent; now there
  *     is nothing to correlate them on, so each is failed at issue time and none reaches the broker.
  *   - `keyless under value matching` is a guard on the fix rather than a gate. It passed before the fix too, because the
  *     matcher never looked at the key. It is here so that carrying the key as `null` cannot silently break correlation that
  *     already worked — which is the obvious way to get this fix wrong.
  */
class KeylessCorrelationSpec extends munit.FunSuite with TestContainerForAll with StrictLogging {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  /** Generous on purpose: nothing here should time out, so a timeout means a lost or misrouted reply. */
  private val ReplyTimeout: FiniteDuration = 30.seconds

  private val Requests = 24

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  private def consumerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
  )

  private def createTopics(bootstrap: String, names: String*): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap).asJava,
    )
    try admin.createTopics(names.map(new NewTopic(_, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
    finally admin.close()
  }

  private final class SystemClock extends Clock {
    override def nowMillis: Long = System.currentTimeMillis()
  }

  /** Records *which* virtual users were advanced, not just how many.
    *
    * A bare counter cannot see the defect this suite exists for. A correlation table that resolved every reply against the
    * wrong pending request still advances the right number of users — 24 completions, 24 OK, zero timeouts — and only the
    * identities are wrong. Keeping the ids is what makes cross-attribution observable.
    */
  private final class CountingAction extends Action {
    private val users = ConcurrentHashMap.newKeySet[Long]()
    private val count = new AtomicInteger(0)

    override def name: String                    = "counting-next"
    override def !(session: Session): Unit       = execute(session)
    override def execute(session: Session): Unit = { users.add(session.userId); count.incrementAndGet(); () }

    def completed: Int           = count.get()
    def distinctUsers: Set[Long] = users.asScala.toSet
  }

  private def attributes: KafkaAttributes[Array[Byte], Array[Byte]] =
    KafkaAttributes[Array[Byte], Array[Byte]](
      requestName = _ => "request-reply".success,
      producerTopic = None,
      consumerTopic = None,
      key = None,
      value = _ => Array.emptyByteArray.success,
      headers = None,
      keySerde = None,
      valueSerde = Serdes.ByteArray(),
      checks = Nil,
    )

  /** Runs `body` with a request-reply action wired to a real broker, plus an echo responder on `requestTopic` that mirrors
    * every record onto `replyTopic`. `received` counts what the responder saw, which is how "nothing reached the broker" is
    * asserted.
    */
  private def withRig(bootstrap: String, requestTopic: String, replyTopic: String, matcher: KafkaMatcher)(
      body: (KafkaRequestReplyAction[Array[Byte], Array[Byte]], RecordingStatsEngine, CountingAction, AtomicInteger) => Unit,
  ): Unit = {
    createTopics(bootstrap, requestTopic, replyTopic)

    val actorSystem = new ActorSystem()
    val statsEngine = new RecordingStatsEngine
    val clock       = new SystemClock
    val sender      = KafkaSender(producerSettings(bootstrap))
    val next        = new CountingAction
    val received    = new AtomicInteger(0)

    val echoSender      = KafkaSender(producerSettings(bootstrap))
    val echoReady       = new CountDownLatch(1)
    val responder       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
      consumerSettings(bootstrap) + (ConsumerConfig.GROUP_ID_CONFIG -> s"$requestTopic-responder"),
      Set(requestTopic),
      record => {
        received.incrementAndGet()
        echoReady.countDown()
        echoSender.send(KafkaProtocolMessage(record.key(), record.value(), replyTopic, replyTopic))(
          _ => (),
          error => logger.error("[keyless responder] failed to echo", error),
        )
      },
      error => logger.error("[keyless responder] consumer failed", error),
    )
    val responderThread = new Thread(responder, s"$requestTopic-responder")
    responderThread.setDaemon(true)
    responderThread.start()

    try {
      val pool           = new KafkaMessageTrackerPool(consumerSettings(bootstrap), actorSystem, statsEngine, clock)
      val protocol       = KafkaProtocol(
        producerProperties = producerSettings(bootstrap),
        consumerProperties = consumerSettings(bootstrap),
        timeout = ReplyTimeout,
        messageMatcher = matcher,
      )
      val coreComponents =
        new CoreComponents(actorSystem, null, null, None, statsEngine, clock, null, GatlingConfiguration.loadForTest())
      val action         = new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
        KafkaComponents(coreComponents, protocol, Some(pool), sender),
        attributes,
        coreComponents,
        next,
        None,
      )

      // Warm the responder so a skipped first echo is never mistaken for a correlation failure. The probe
      // carries a key, so it is unaffected by what is under test.
      val warmup = KafkaProtocolMessage("warmup".getBytes, "warmup".getBytes, requestTopic, replyTopic)
      val sent   = new CountDownLatch(1)
      sender.send(warmup)(_ => sent.countDown(), _ => sent.countDown())
      assert(sent.await(30, TimeUnit.SECONDS), "warmup request was never delivered")
      assert(echoReady.await(60, TimeUnit.SECONDS), "responder never received anything")
      received.set(0)

      body(action, statsEngine, next, received)
    } finally {
      responder.close()
      echoSender.close()
      sender.close()
      actorSystem.close()
    }
  }

  private def awaitResponses(
      statsEngine: RecordingStatsEngine,
      expected: Int,
      within: FiniteDuration,
  ): Vector[LoggedResponse] = {
    val deadline = System.currentTimeMillis() + within.toMillis
    while (statsEngine.responses.get().size < expected && System.currentTimeMillis() < deadline)
      Thread.sleep(100)
    statsEngine.responses.get()
  }

  test("concurrent keyless requests under key matching are each failed at issue time, and none is sent") {
    withContainers { kafka =>
      withRig(kafka.bootstrapServers, "keyless-key-request", "keyless-key-reply", KafkaKeyMatcher) {
        (action, statsEngine, next, received) =>
          // Issued from several threads at once: the defect needed two keyless requests in flight
          // together, since the second is what displaced the first.
          val threads = (1 to Requests).map { i =>
            val t = new Thread(() =>
              action.sendKafkaMessage(
                "request-reply",
                KafkaProtocolMessage(null, s"body-$i".getBytes, "keyless-key-request", "keyless-key-reply"),
                Session("scenario", i.toLong, null),
              ),
            )
            t.setDaemon(true)
            t
          }
          threads.foreach(_.start())
          threads.foreach(_.join(30000))

          val responses = awaitResponses(statsEngine, Requests, 60.seconds)
          assertEquals(responses.size, Requests, "every request must reach a terminal outcome")

          val uncorrelatable = responses.count(_.message.exists(_.contains("supplies no value")))
          assertEquals(
            uncorrelatable,
            Requests,
            "every keyless request under key matching must be failed for having nothing to correlate on; " +
              s"instead: ${responses.flatMap(_.message).distinct.mkString(" | ")}",
          )
          assertEquals(
            responses.count(_.status == (OK: Status)),
            0,
            "none may be reported as a success: before the fix a reply resolved whichever request held the shared slot",
          )
          // Settle first. Every one of these KOs is reported synchronously inside sendKafkaMessage, so
          // without a wait `received` is sampled milliseconds after the last thread died — before a
          // produce round trip plus the responder's poll cycle could have shown anything. That would let
          // a "report the failure but send it anyway" regression pass.
          Thread.sleep(5000)
          assertEquals(
            received.get(),
            0,
            "and none may reach the broker: a request whose reply could never be matched must not be published",
          )
          assertEquals(next.completed, Requests, "every virtual user must be advanced exactly once")
      }
    }
  }

  test("concurrent keyless requests still correlate when the matcher uses a field they carry") {
    withContainers { kafka =>
      withRig(kafka.bootstrapServers, "keyless-value-request", "keyless-value-reply", KafkaValueMatcher) {
        (action, statsEngine, next, received) =>
          val threads = (1 to Requests).map { i =>
            val t = new Thread(() =>
              action.sendKafkaMessage(
                "request-reply",
                KafkaProtocolMessage(null, s"body-$i".getBytes, "keyless-value-request", "keyless-value-reply"),
                Session("scenario", i.toLong, null),
              ),
            )
            t.setDaemon(true)
            t
          }
          threads.foreach(_.start())
          threads.foreach(_.join(30000))

          val responses = awaitResponses(statsEngine, Requests, ReplyTimeout + 60.seconds)
          assertEquals(responses.size, Requests, s"only ${responses.size} of $Requests requests reported an outcome")

          val timedOut = responses.filter(_.message.exists(_.startsWith("Reply timeout")))
          assertEquals(
            timedOut.size,
            0,
            s"${timedOut.size} of $Requests replies were lost: the responder answered every request, so a timeout " +
              "here means the reply was not matched to the request that earned it",
          )
          assert(
            responses.forall(_.status == (OK: Status)),
            s"unexpected failures: ${responses.filterNot(_.status == (OK: Status)).flatMap(_.message).distinct.mkString(" | ")}",
          )
          assertEquals(received.get(), Requests, "every request must have reached the broker")
          assertEquals(next.completed, Requests, "every virtual user must be advanced exactly once")
          assertEquals(
            next.distinctUsers,
            (1L to Requests.toLong).toSet,
            "each of the distinct virtual users must be advanced: a correlation table that resolved every reply " +
              "against the wrong request would still advance the right *number* of users",
          )
      }
    }
  }

  /** Issue #167, the produce side. A request that supplies no key must reach the broker with `key == null`, not with an empty
    * byte array.
    *
    * The distinction is not cosmetic. An empty key is a *present* key: Kafka hashes it, and `murmur2` of an empty input is a
    * constant, so every keyless record landed on one partition for the whole run. A null key is absent, which is what lets
    * Kafka apply its keyless strategy at all — and what a log-compacted topic rejects, which is why the Migration Guide has to
    * mention it.
    *
    * Deliberately NOT asserted here: which partitions the records land on. Distribution is Kafka's decision and it changed in
    * 3.3 (the built-in partitioner now batches keyless records stickily rather than round-robin), so asserting spread would
    * test Kafka and depend on batch size and timing. What the plugin controls is the key on the wire.
    *
    * Driven through `sendRequest`, not `sendKafkaMessage`: the substitution lived in `KafkaAction.resolveToProtocolMessage`,
    * which only the former runs. Handing a pre-built `KafkaProtocolMessage` to the latter would bypass the code under test.
    */
  test("a request that supplies no key is published with no key") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val topic     = "keyless-produce"
      createTopics(bootstrap, topic)

      val actorSystem = new ActorSystem()
      val statsEngine = new RecordingStatsEngine
      val sender      = KafkaSender(producerSettings(bootstrap))
      val next        = new CountingAction
      try {
        val protocol       = KafkaProtocol(
          producerProperties = producerSettings(bootstrap),
          consumerProperties = consumerSettings(bootstrap),
          timeout = ReplyTimeout,
          messageMatcher = KafkaKeyMatcher,
        )
        val coreComponents = new CoreComponents(
          actorSystem,
          null,
          null,
          None,
          statsEngine,
          new SystemClock,
          null,
          GatlingConfiguration.loadForTest(),
        )
        // key = None is what the DSL produces for `send[K, V](null, payload)`, since `Option(null)` is `None`.
        val keyless        = attributes.copy(producerTopic = Some(_ => topic.success))
        val action         = new KafkaRequestAction[Array[Byte], Array[Byte]](
          KafkaComponents(coreComponents, protocol, None, sender),
          keyless,
          coreComponents,
          next,
          None,
        )

        (1 to Requests).foreach(i => action.sendRequest(Session("scenario", i.toLong, null)))

        val deadline = System.currentTimeMillis() + 60000
        while (statsEngine.responses.get().size < Requests && System.currentTimeMillis() < deadline)
          Thread.sleep(100)
        assertEquals(statsEngine.responses.get().size, Requests, "every send must be reported")

        val keys = readKeys(bootstrap, topic, Requests)
        assertEquals(keys.size, Requests, s"expected to read back $Requests records, got ${keys.size}")
        assertEquals(
          keys.count(_ != null),
          0,
          "a request with no key must be published with a null key: an empty byte array is a present key, it hashes to a " +
            "constant, and a compacted topic rejects the record outright",
        )
      } finally {
        sender.close()
        actorSystem.close()
      }
    }
  }

  /** Reads exactly `expected` records from the start of a freshly created topic, failing rather than returning a short list.
    *
    * Bounded by count, not only by a deadline: a read that fell short and returned what it had would let an "all keys absent"
    * assertion pass having inspected one record.
    */
  private def readKeys(bootstrap: String, topic: String, expected: Int): List[Array[Byte]] = {
    val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[Array[Byte], Array[Byte]](
      (consumerSettings(bootstrap) + (ConsumerConfig.GROUP_ID_CONFIG -> s"$topic-key-reader")).asJava,
    )
    try {
      consumer.subscribe(List(topic).asJava)
      val collected = scala.collection.mutable.ListBuffer.empty[Array[Byte]]
      val deadline  = System.currentTimeMillis() + 60000
      while (collected.size < expected && System.currentTimeMillis() < deadline)
        consumer.poll(java.time.Duration.ofMillis(500)).forEach(r => collected += r.key())
      collected.toList
    } finally consumer.close()
  }
}
