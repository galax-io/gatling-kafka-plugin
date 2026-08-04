package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.munit.TestContainerForAll
import com.dimafeng.testcontainers.{ConfluentKafkaContainer, ContainerDef}
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
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyAction
import org.galaxio.gatling.kafka.client.{KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMatcher}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaAttributes
import org.testcontainers.utility.DockerImageName

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Lifetime coverage for issue #165 — a reply channel and its tracker registration must survive the completion of the requests
  * that created them, and live for the whole simulation run.
  *
  * The broker is started with a deliberately long `group.initial.rebalance.delay.ms`, so establishing a reply channel is
  * genuinely expensive. That is what makes the defect visible: before the fix, every matched reply released the tracker and
  * unsubscribed the topic, emptying the consumer group, so the next request re-joined and paid the delay again.
  *
  * These tests drive the real [[KafkaRequestReplyAction]] rather than the pool directly, because the release being removed is
  * wired inside the action. A pool-level test could not trigger it without naming symbols that stop existing, and would
  * therefore not compile on both sides of the change.
  */
class TrackerLifetimeSpec extends munit.FunSuite with TestContainerForAll {

  /** How long the broker holds back the first assignment of a new consumer group. */
  private val AssignmentStall: FiniteDuration = 5.seconds

  /** A request that reuses an established channel must complete far inside the stall above. */
  private val ReuseBudget: FiniteDuration = 1500.millis

  override val containerDef: ContainerDef { type Container = ConfluentKafkaContainer } =
    new ContainerDef {
      override type Container = ConfluentKafkaContainer

      override def createContainer(): ConfluentKafkaContainer = {
        val container = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.5"))
        container.container.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", AssignmentStall.toMillis.toString)
        container
      }
    }

  // The red run pays one full assignment stall per request; 50 sequential requests is ~4 minutes of it.
  override def munitTimeout: scala.concurrent.duration.Duration = 10.minutes

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  // No group id on purpose: the pool then generates a random one, so every pool joins as a brand new
  // group and pays the broker's initial rebalance delay on first subscription.
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
    try
      admin
        .createTopics(names.map(new NewTopic(_, 1, 1.toShort)).asJava)
        .all()
        .get(30, TimeUnit.SECONDS)
    finally admin.close()
  }

  private final class SystemClock extends Clock {
    override def nowMillis: Long = System.currentTimeMillis()
  }

  /** Counts how many sessions the action passed on. Only used to satisfy `next`; assertions read the stats engine, which is
    * what a user actually sees.
    */
  private final class CountingAction extends Action {
    private val count = new AtomicInteger(0)

    override def name: String = "counting-next"

    override def !(session: Session): Unit = execute(session)

    override def execute(session: Session): Unit = { count.incrementAndGet(); () }

    def completed: Int = count.get()
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

  /** Everything one simulation lifetime owns. Tests that assert on teardown construct two of these. */
  private final class Run(
      bootstrap: String,
      val matcher: KafkaMatcher,
      replyTimeout: FiniteDuration = 60.seconds,
      idleGrace: FiniteDuration = 30.seconds,
  ) {
    val actorSystem: ActorSystem          = new ActorSystem()
    val statsEngine: RecordingStatsEngine = new RecordingStatsEngine
    val sender: KafkaSender               = KafkaSender(producerSettings(bootstrap))
    val clock: Clock                      = new SystemClock
    val pool: KafkaMessageTrackerPool     = {
      val p = new KafkaMessageTrackerPool(consumerSettings(bootstrap), actorSystem, statsEngine, clock)
      p.idleGraceMillis = idleGrace.toMillis
      p
    }
    val next: CountingAction              = new CountingAction

    private val protocol =
      KafkaProtocol(
        producerProperties = producerSettings(bootstrap),
        consumerProperties = consumerSettings(bootstrap),
        timeout = replyTimeout,
        messageMatcher = matcher,
      )

    // The request-reply path reads only `statsEngine` and `clock` off CoreComponents; the rest is
    // never touched, so it is left null rather than faked.
    private val coreComponents =
      new CoreComponents(
        actorSystem,
        null,
        null,
        None,
        statsEngine,
        clock,
        null,
        GatlingConfiguration.loadForTest(),
      )

    val action: KafkaRequestReplyAction[Array[Byte], Array[Byte]] =
      new KafkaRequestReplyAction[Array[Byte], Array[Byte]](
        KafkaComponents(coreComponents, protocol, Some(pool), sender),
        attributes,
        coreComponents,
        next,
        None,
      )

    def responses: Vector[LoggedResponse] = statsEngine.responses.get()

    def close(): Unit = {
      sender.close()
      actorSystem.close()
    }
  }

  private def withRun(bootstrap: String, matcher: KafkaMatcher = KafkaKeyMatcher)(body: Run => Unit): Unit = {
    val run = new Run(bootstrap, matcher)
    try body(run)
    finally run.close()
  }

  private def withRunReturning[A](bootstrap: String, matcher: KafkaMatcher = KafkaKeyMatcher)(body: Run => A): A = {
    val run = new Run(bootstrap, matcher)
    try body(run)
    finally run.close()
  }

  /** Reads the pool's registration for `(topic, matcher)` without naming its type.
    *
    * Deliberately returns `AnyRef`: before the change the map holds a `TrackerEntry`, after it an `ActorRef`. Naming either
    * would make this file compile against only one of them, and the whole point is that the same test body runs red before and
    * green after.
    */
  private def registrationFor(pool: KafkaMessageTrackerPool, topic: String, matcher: KafkaMatcher): AnyRef = {
    val field = classOf[KafkaMessageTrackerPool].getDeclaredField("trackers")
    field.setAccessible(true)
    val outer = field.get(pool).asInstanceOf[java.util.Map[String, java.util.Map[AnyRef, AnyRef]]]
    val inner = outer.get(topic)
    if (inner == null) null
    else {
      // Keys are the pool's private matcher wrapper; unwrap by reflection so distinct matcher
      // instances on one topic stay distinguishable.
      var found: AnyRef = null
      inner.forEach { (key, value) =>
        val matcherField = key.getClass.getDeclaredField("matcher")
        matcherField.setAccessible(true)
        if (matcherField.get(key).asInstanceOf[AnyRef] eq matcher) found = value
      }
      found
    }
  }

  private def message(requestTopic: String, replyTopic: String, key: String): KafkaProtocolMessage =
    KafkaProtocolMessage(key.getBytes, "request".getBytes, requestTopic, replyTopic)

  private def session: Session = Session("scenario", 1L, null)

  private def produce(sender: KafkaSender, topic: String, key: String, value: String): Unit = {
    val latch = new CountDownLatch(1)
    sender.send(KafkaProtocolMessage(key.getBytes, value.getBytes, topic, topic))(
      _ => latch.countDown(),
      _ => latch.countDown(),
    )
    assert(latch.await(30, TimeUnit.SECONDS), s"producing onto $topic never completed")
  }

  private def send(run: Run, name: String, requestTopic: String, replyTopic: String, key: String): Unit =
    run.action.sendKafkaMessage(name, message(requestTopic, replyTopic, key), session)

  /** Responses are counted per request name so two scenarios can share one pool and one action. */
  private def responsesNamed(run: Run, name: String): Int = run.responses.count(_.requestName == name)

  private def median(values: Seq[Long]): Long = {
    val sorted = values.sorted
    sorted(sorted.size / 2)
  }

  /** Waits for the pool to hold a registration for `(replyTopic, matcher)` and returns it, or null on timeout. */
  private def awaitRegistration(run: Run, replyTopic: String, timeout: FiniteDuration = 2.minutes): AnyRef = {
    val deadline      = System.currentTimeMillis() + timeout.toMillis
    var found: AnyRef = registrationFor(run.pool, replyTopic, run.matcher)
    while (found == null && System.currentTimeMillis() < deadline) {
      Thread.sleep(10)
      found = registrationFor(run.pool, replyTopic, run.matcher)
    }
    found
  }

  /** Re-publishes the reply until the request is reported.
    *
    * A property of the harness, not a workaround for a product defect. There is no responder here, and `send` returns as soon
    * as the request is handed off — the request itself is only published once the reply channel has been established, which on
    * a first use is a full assignment stall away. A reply produced before that has nothing subscribed to receive it and is
    * simply never delivered, however correct the plugin is.
    *
    * It used to cover for issue #191 as well, where a reply arriving before its own request finished registering was dropped
    * silently. That half is gone: registration now precedes the send, so a reply produced after the channel exists is always
    * matched.
    */
  private def driveReply(
      run: Run,
      name: String,
      replyTopic: String,
      key: String,
      expectedTotal: Int,
      timeout: FiniteDuration,
  ): Unit = {
    val deadline = System.currentTimeMillis() + timeout.toMillis
    while (responsesNamed(run, name) < expectedTotal && System.currentTimeMillis() < deadline) {
      produce(run.sender, replyTopic, key, "reply")
      val nudge = System.currentTimeMillis() + 500
      while (responsesNamed(run, name) < expectedTotal && System.currentTimeMillis() < nudge) Thread.sleep(25)
    }
    assert(
      responsesNamed(run, name) >= expectedTotal,
      s"expected $expectedTotal '$name' responses on $replyTopic, got ${responsesNamed(run, name)}",
    )
  }

  private def requestReply(
      run: Run,
      name: String,
      requestTopic: String,
      replyTopic: String,
      key: String,
      timeout: FiniteDuration = 2.minutes,
  ): Long = {
    val expected  = responsesNamed(run, name) + 1
    val startedAt = System.currentTimeMillis()
    send(run, name, requestTopic, replyTopic, key)
    driveReply(run, name, replyTopic, key, expected, timeout)
    System.currentTimeMillis() - startedAt
  }

  test("the harness drives a real request-reply end to end") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "lifetime-smoke-request"
      val replyTopic   = "lifetime-smoke-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withRun(bootstrap) { run =>
        send(run, "smoke", requestTopic, replyTopic, "smoke-key")

        // Read the registration while the request is still in flight — before any reply is produced.
        // This is what proves the reflection helper reads real pool state rather than always
        // returning null; asserting after completion would be asserting the defect itself, which is
        // the lifetime tests' job, not the harness smoke test's.
        assert(
          awaitRegistration(run, replyTopic) != null,
          "the pool never registered a tracker for the reply topic, or the reflection helper cannot see it",
        )

        driveReply(run, "smoke", replyTopic, "smoke-key", expectedTotal = 1, timeout = 2.minutes)

        assertEquals(run.responses.size, 1)
        assertEquals(run.responses.head.status, OK: Status, s"unexpected outcome: ${run.responses.head}")
        assertEquals(run.next.completed, 1, "the action did not pass the session on to next")
      }
    }
  }

  test("(1) a tracker registration survives the completion of the request that created it") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "lifetime-survive-request"
      val replyTopic   = "lifetime-survive-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withRun(bootstrap) { run =>
        send(run, "survive", requestTopic, replyTopic, "survive-key")
        val whileInFlight = awaitRegistration(run, replyTopic)
        assert(whileInFlight != null, "no registration appeared while the request was in flight")

        driveReply(run, "survive", replyTopic, "survive-key", expectedTotal = 1, timeout = 2.minutes)
        assertEquals(run.responses.head.status, OK: Status, s"unexpected outcome: ${run.responses.head}")

        // Timing-independent: the registration is either still there or it is not.
        val afterCompletion = registrationFor(run.pool, replyTopic, run.matcher)
        assert(
          afterCompletion != null,
          "the registration was dropped when the request completed — the reply channel is scoped to in-flight requests",
        )
        assert(
          afterCompletion eq whileInFlight,
          "the registration was replaced rather than held across the request's completion",
        )
      }
    }
  }

  test("(2) a reply channel survives an idle gap and is reused by the next request") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "lifetime-reuse-request"
      val replyTopic   = "lifetime-reuse-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withRun(bootstrap) { run =>
        send(run, "reuse", requestTopic, replyTopic, "reuse-1")
        val established = awaitRegistration(run, replyTopic)
        assert(established != null, "no registration appeared for the first request")
        driveReply(run, "reuse", replyTopic, "reuse-1", expectedTotal = 1, timeout = 2.minutes)

        // Longer than the consumer's 1 s poll cycle, so a queued unsubscribe is actually applied
        // rather than coalescing with the next subscribe into an unchanged topic set — which would
        // hide the churn. The gap is also a scenario in its own right: any profile with pacing or
        // think time produces exactly this.
        Thread.sleep(3000)

        assert(
          registrationFor(run.pool, replyTopic, run.matcher) eq established,
          "the reply channel did not survive a 3 s idle gap: the next request has to re-establish it",
        )

        val elapsed = requestReply(run, "reuse", requestTopic, replyTopic, "reuse-2")
        assertEquals(responsesNamed(run, "reuse"), 2)
        assert(
          registrationFor(run.pool, replyTopic, run.matcher) eq established,
          "the reply channel was replaced while serving the second request",
        )
        // Loose upper guard only. Measured pre-change cost of a re-establishment after the first
        // group join is ~0.6 s, not the full initial-rebalance delay, so this bound is a sanity
        // check rather than the discriminating assertion — the registration identity above is.
        assert(
          elapsed < AssignmentStall.toMillis,
          s"the second request took $elapsed ms, longer than a full assignment stall",
        )
      }
    }
  }

  test("(3) establishment happens once across a sequential scenario") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "lifetime-sequential-request"
      val replyTopic   = "lifetime-sequential-reply"
      val requests     = 50
      createTopics(bootstrap, requestTopic, replyTopic)

      withRun(bootstrap) { run =>
        send(run, "seq", requestTopic, replyTopic, "seq-1")
        val first = awaitRegistration(run, replyTopic)
        assert(first != null, "no registration appeared for the first request")
        driveReply(run, "seq", replyTopic, "seq-1", expectedTotal = 1, timeout = 2.minutes)

        (2 to requests).foreach(i => requestReply(run, "seq", requestTopic, replyTopic, s"seq-$i"))

        assertEquals(responsesNamed(run, "seq"), requests)
        assert(
          run.responses.forall(_.status == (OK: Status)),
          s"not every request succeeded: ${run.responses.filterNot(_.status == (OK: Status))}",
        )
        assert(
          registrationFor(run.pool, replyTopic, run.matcher) eq first,
          s"the reply channel was re-established during a $requests-request sequential scenario",
        )
      }
    }
  }

  private def liveThreadNamed(prefix: String): Boolean =
    Thread.getAllStackTraces.keySet().asScala.exists(t => t.isAlive && t.getName.startsWith(prefix))

  test("(5) a run releases everything it held, and a second run in the same process is unaffected") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val request   = "lifetime-teardown-request"
      val reply     = "lifetime-teardown-reply"
      createTopics(bootstrap, request, reply)

      val first = new Run(bootstrap, KafkaKeyMatcher)
      try requestReply(first, "first", request, reply, "first-key")
      finally first.close()

      // The setup executor is the one pool-owned thread with a naming factory, so it is the only
      // one that can be attributed to a lifetime by name. Give teardown a moment to land.
      val deadline = System.currentTimeMillis() + 10000
      while (liveThreadNamed("gatling-kafka-tracker-setup") && System.currentTimeMillis() < deadline) Thread.sleep(50)
      assert(
        !liveThreadNamed("gatling-kafka-tracker-setup"),
        "a tracker setup thread outlived the run that created it",
      )

      // The consumer uses the JDK default thread factory, so it cannot be identified by name.
      // Observe it directly instead: the pool's consumer task must have finished. A late reply on
      // the topic proves nothing here — the request already completed, so its key is gone from
      // sentMessages and the actor is terminated, meaning the assertion would pass whether or not
      // the consumer leaked.
      val consumerFuture = classOf[KafkaMessageTrackerPool].getDeclaredField("consumerFuture")
      consumerFuture.setAccessible(true)
      val future         = consumerFuture.get(first.pool).asInstanceOf[java.util.concurrent.Future[_]]
      assert(future.isDone, "the pool's consumer task was still running after its run was closed")

      // The periodic timeout scan is a separate lifetime (issue #166 owns its Cancellable): a timer
      // still firing after teardown would log a KO into this stats engine. The scan interval is 1 s,
      // so a 3 s window gives it several chances.
      val settled = first.responses.size
      Thread.sleep(3000)
      assertEquals(
        first.responses.size,
        settled,
        s"the closed run kept reporting: ${first.responses.drop(settled)}",
      )

      // A second run over the same broker builds its own channel and behaves identically.
      withRun(bootstrap) { second =>
        requestReply(second, "second", request, reply, "second-key")
        assertEquals(responsesNamed(second, "second"), 1)
        assertEquals(second.responses.head.status, OK: Status, s"unexpected outcome: ${second.responses.head}")
        assert(
          registrationFor(second.pool, reply, second.matcher) != null,
          "the second run did not establish its own registration",
        )
      }
    }
  }

  /** Cross-topic guard for SC-003.
    *
    * Classified as a guard, not a red-first test: measured against the pre-change code in this environment, scenario B's churn
    * did not move scenario A's median outside the 1.5x bound. Re-establishing a subscription after the first group join costs
    * roughly 0.6 s here, not the full initial-rebalance delay the plan assumed, so the cross-topic effect stays below the
    * threshold. The test still earns its place — it fails if a future change makes one scenario's cadence stall another's reply
    * detection — but it does not demonstrate the defect.
    */
  test("(6) an idle reply channel is released and its topic unsubscribed after the grace period") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "lifetime-idle-request"
      val replyTopic   = "lifetime-idle-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      // Reply timeout stays generous so the first establishment can finish; the grace is what this
      // test is about and is set short on its own.
      val grace = 3.seconds
      val run   = new Run(bootstrap, KafkaKeyMatcher, idleGrace = grace)
      try {
        requestReply(run, "idle", requestTopic, replyTopic, "idle-key")
        assert(
          registrationFor(run.pool, replyTopic, run.matcher) != null,
          "the channel was released while its request was still completing",
        )

        // This is the regression guard for issue #78: a reply topic derived per virtual user goes
        // idle after its single request and must be reclaimed, or a run with per-user reply topics
        // accumulates one subscription, one actor and one timeout scan per user until the consumer
        // group metadata exceeds the broker's limit and the run cannot recover.
        val deadline = System.currentTimeMillis() + grace.toMillis + 10000
        while (registrationFor(run.pool, replyTopic, run.matcher) != null && System.currentTimeMillis() < deadline)
          Thread.sleep(100)

        assert(
          registrationFor(run.pool, replyTopic, run.matcher) == null,
          s"the channel was still held ${System.currentTimeMillis() - deadline + 10000} ms after going idle",
        )

        // Released, not broken: a later request rebuilds the channel and still works.
        requestReply(run, "idle", requestTopic, replyTopic, "idle-key-2")
        assertEquals(responsesNamed(run, "idle"), 2)
        assert(
          run.responses.forall(_.status == (OK: Status)),
          s"a request after re-establishment failed: ${run.responses.filterNot(_.status == (OK: Status))}",
        )
      } finally run.close()
    }
  }

  /** The tracker actor instance behind a pool registration.
    *
    * Two reflection hops, both into classpath classes: `TrackerEntry.actor` gives Gatling's `ActorRef`, whose implementation
    * holds the `Actor` instance. Needed because the pool creates its trackers internally, so a test never sees the instance —
    * and the instance is where the evidence for issue #166 lives.
    */
  private def trackerBehind(registration: AnyRef): AnyRef = {
    val entryField = registration.getClass.getDeclaredField("actor")
    entryField.setAccessible(true)
    val ref        = entryField.get(registration)
    val instField  = ref.getClass.getDeclaredField("actor")
    instField.setAccessible(true)
    instField.get(ref)
  }

  /** Whether a tracker is still holding an armed periodic timeout scan.
    *
    * The scan is the leak: it captures `self`, and Gatling's scheduler is a single thread shared by the whole simulation, so
    * one left armed after its channel is released keeps the tracker — with its stats engine, clock and matcher closures —
    * reachable, and costs a wakeup per second for the rest of the run. Read here rather than off the scheduler, whose
    * ScheduledThreadPoolExecutor sits behind a JDK-internal delegate that JDK 17 will not open to reflection.
    */
  private def scanIsArmed(tracker: AnyRef): Boolean = {
    val field = tracker.getClass.getDeclaredField("periodicTimeoutScan")
    field.setAccessible(true)
    field.get(tracker).asInstanceOf[Option[_]].isDefined
  }

  test("(7) a released channel takes its timeout scan with it, so scans track channels held not channels created") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      // Enough distinct reply topics that "one scan per channel ever created" and "one scan per channel
      // currently held" are unmistakably different numbers. This is the shape issue #78 requires to stay
      // bounded — a reply topic derived per virtual user — and the shape #166 made unbounded again by
      // releasing the registration while leaving everything hanging off it running.
      val channels  = 20
      val grace     = 3.seconds
      val topics    = (1 to channels).flatMap(i => List(s"lifetime-scan-request-$i", s"lifetime-scan-reply-$i"))
      createTopics(bootstrap, topics: _*)

      val run = new Run(bootstrap, KafkaKeyMatcher, idleGrace = grace)
      try {
        // Captured as each channel is created: after release the registration is gone and the tracker
        // with it, so there would be nothing left to inspect.
        val trackers = (1 to channels).map { i =>
          val replyTopic   = s"lifetime-scan-reply-$i"
          requestReply(run, s"scan-$i", s"lifetime-scan-request-$i", replyTopic, s"scan-key-$i")
          val registration = registrationFor(run.pool, replyTopic, run.matcher)
          assert(registration != null, s"channel $i was released while its request was still completing")
          val tracker      = trackerBehind(registration)
          assert(scanIsArmed(tracker), s"channel $i never armed a timeout scan, so this proves nothing")
          tracker
        }

        val deadline = System.currentTimeMillis() + grace.toMillis + 30000
        while (
          (1 to channels).exists(i => registrationFor(run.pool, s"lifetime-scan-reply-$i", run.matcher) != null) &&
          System.currentTimeMillis() < deadline
        ) Thread.sleep(100)

        val stillHeld = (1 to channels).count(i => registrationFor(run.pool, s"lifetime-scan-reply-$i", run.matcher) != null)
        assertEquals(stillHeld, 0, "every channel should have gone idle and been released by now")

        val stillArmed = trackers.count(scanIsArmed)
        assertEquals(
          stillArmed,
          0,
          s"$stillArmed of $channels released channels left their timeout scan running; scans must be bounded by " +
            "the channels currently held, not by the number ever created",
        )
      } finally run.close()
    }
  }

  test("(4) a second scenario looping on its own topic pair does not disturb the first") {
    withContainers { kafka =>
      val bootstrap = kafka.bootstrapServers
      val aRequest  = "lifetime-cross-a-request"
      val aReply    = "lifetime-cross-a-reply"
      val bRequest  = "lifetime-cross-b-request"
      val bReply    = "lifetime-cross-b-reply"
      val samples   = 20
      createTopics(bootstrap, aRequest, aReply, bRequest, bReply)

      // Baseline: scenario A alone. The first request establishes the channel and is excluded.
      val solo = withRunReturning(bootstrap) { run =>
        requestReply(run, "a", aRequest, aReply, "a-warmup")
        median((1 to samples).map(i => requestReply(run, "a", aRequest, aReply, s"a-solo-$i")))
      }

      // Same measurement while scenario B repeatedly completes and restarts on its own topic pair.
      val combined = withRunReturning(bootstrap) { run =>
        requestReply(run, "a", aRequest, aReply, "a-warmup")

        @volatile var stop = false
        val bFailures      = new AtomicInteger(0)
        val looper         = new Thread(
          () =>
            try {
              var i = 0
              // Paced and bounded on purpose. Once the channel is held, a B request completes in
              // milliseconds, so an unthrottled loop would generate tens of thousands of responses
              // and exhaust the test JVM before it proved anything. Pacing is also what a real
              // scenario has.
              while (!stop && i < 200) {
                i += 1
                requestReply(run, "b", bRequest, bReply, s"b-$i", timeout = 1.minute)
                Thread.sleep(50)
              }
            } catch { case _: Throwable => bFailures.incrementAndGet() },
          "cross-topic-b-looper",
        )
        looper.setDaemon(true)
        looper.start()

        val result =
          try median((1 to samples).map(i => requestReply(run, "a", aRequest, aReply, s"a-combined-$i")))
          finally { stop = true; looper.join(30000) }

        // Without this the guard is vacuous: if B dies on its first iteration the catch above absorbs
        // it and A is measured running alone, which passes trivially — including in the very
        // situation the test exists to detect.
        // Liveness, not throughput. B's completed count is a function of machine speed — it is 20+
        // locally and 3 on CI — so asserting a count is asserting how fast the runner is. What the
        // guard actually needs is that B ran continuously: it exited only via the stop flag or its
        // iteration cap (bFailures == 0, since any abort lands in the catch) and it was genuinely
        // producing traffic (at least one completed round trip). Both hold regardless of speed.
        assertEquals(bFailures.get(), 0, "the competing scenario aborted, so it was not competing")
        assert(
          responsesNamed(run, "b") >= 1,
          "the competing scenario completed no requests at all; A was not measured under load",
        )
        result
      }

      assert(
        combined <= math.max(solo * 3 / 2, ReuseBudget.toMillis),
        s"scenario A's median response time went from $solo ms alone to $combined ms alongside scenario B: " +
          s"B's channel churn is stalling the shared reply consumer",
      )
    }
  }
}
