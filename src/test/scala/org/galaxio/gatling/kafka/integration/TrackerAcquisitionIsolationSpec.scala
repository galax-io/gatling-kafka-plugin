package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.munit.TestContainerForAll
import com.dimafeng.testcontainers.{ConfluentKafkaContainer, ContainerDef}
import io.gatling.commons.stats.{OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.actor.{ActorRef, ActorSystem}
import io.gatling.core.session.Session
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaMessageTracker, KafkaMessageTrackerPool, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMatcher, KafkaMessageMatcher}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.testcontainers.utility.DockerImageName

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Regression coverage for issue #163 — tracker acquisition must not run its wait on the producer's I/O callback thread.
  *
  * The broker is started with a deliberately long `group.initial.rebalance.delay.ms`, so the first subscription of every
  * freshly created pool (each one uses its own random group id) genuinely takes seconds to be assigned. That is a real
  * broker-side stall, not an injected delay, and it is exactly the condition under which the reported defect froze the shared
  * producer.
  */
class TrackerAcquisitionIsolationSpec extends munit.FunSuite with TestContainerForAll {

  /** How long the broker holds back the first assignment of a new consumer group. */
  private val AssignmentStall: FiniteDuration = 5.seconds

  /** A callback that merely hands off work must return far faster than the stall above. */
  private val CallbackBudget: FiniteDuration = 1500.millis

  override val containerDef: ContainerDef { type Container = ConfluentKafkaContainer } =
    new ContainerDef {
      override type Container = ConfluentKafkaContainer

      override def createContainer(): ConfluentKafkaContainer = {
        val container = ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.5"))
        container.container.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", AssignmentStall.toMillis.toString)
        container
      }
    }

  override def munitTimeout: scala.concurrent.duration.Duration = 5.minutes

  private def producerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
  )

  // No group id on purpose: the pool then generates a random one, so every pool joins as a brand new
  // group and pays the broker's initial rebalance delay.
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

  /** Runs `body` with a pool and a sender that share one producer, tearing both down afterwards. The pool's stats engine is
    * handed over too, so a test can assert on what the trackers it creates actually report.
    */
  private def withPoolAndSender(
      bootstrap: String,
  )(body: (KafkaMessageTrackerPool, KafkaSender, RecordingStatsEngine) => Unit): Unit = {
    val actorSystem = new ActorSystem()
    val sender      = KafkaSender(producerSettings(bootstrap))
    val statsEngine = new RecordingStatsEngine
    try {
      val pool = new KafkaMessageTrackerPool(
        consumerSettings(bootstrap),
        actorSystem,
        statsEngine,
        new SystemClock,
      )
      body(pool, sender, statsEngine)
    } finally {
      sender.close()
      actorSystem.close()
    }
  }

  private final class RecordingAction extends Action {
    override def name: String = "recording-next"

    val lastSession: AtomicReference[Session] = new AtomicReference[Session]()

    /** Trips when the tracker hands a session on — the completion signal a virtual user actually observes. */
    val completed: CountDownLatch = new CountDownLatch(1)

    override def !(session: Session): Unit = execute(session)

    override def execute(session: Session): Unit = { lastSession.set(session); completed.countDown() }
  }

  private def message(topic: String, key: String, value: String): KafkaProtocolMessage =
    KafkaProtocolMessage(key.getBytes, value.getBytes, topic, topic)

  /** Mirrors what KafkaRequestReplyAction does: acquire the reply tracker from inside the delivery callback. */
  private def sendAndAcquireInCallback(
      sender: KafkaSender,
      pool: KafkaMessageTrackerPool,
      requestTopic: String,
      replyTopic: String,
      timeout: FiniteDuration,
  ): Acquisition = {
    val acquisition = new Acquisition
    sender.send(message(requestTopic, "k", "request"))(
      _ => {
        acquisition.ackAt.set(System.currentTimeMillis())
        acquisition.callbackEntered.countDown()
        pool.acquireTracker(requestTopic, replyTopic, KafkaKeyMatcher, None, timeout)(
          tracker => {
            acquisition.readyAt.set(System.currentTimeMillis())
            acquisition.tracker.set(tracker)
            acquisition.settled.countDown()
          },
          error => {
            acquisition.failedAt.set(System.currentTimeMillis())
            acquisition.failure.set(error)
            acquisition.settled.countDown()
          },
        )
        acquisition.callbackReturnedAt.set(System.currentTimeMillis())
      },
      error => {
        acquisition.failure.set(error)
        acquisition.settled.countDown()
      },
    )
    acquisition
  }

  private final class Acquisition {
    val ackAt: AtomicLong                                                      = new AtomicLong(0L)
    val callbackReturnedAt: AtomicLong                                         = new AtomicLong(0L)
    val readyAt: AtomicLong                                                    = new AtomicLong(0L)
    val failedAt: AtomicLong                                                   = new AtomicLong(0L)
    val tracker: AtomicReference[ActorRef[KafkaMessageTracker.TrackerMessage]] = new AtomicReference(null)
    val failure: AtomicReference[Throwable]                                    = new AtomicReference(null)
    val callbackEntered: CountDownLatch                                        = new CountDownLatch(1)
    val settled: CountDownLatch                                                = new CountDownLatch(1)

    def awaitSettled(): Unit =
      assert(settled.await(2, TimeUnit.MINUTES), "acquisition never completed")

    def awaitCallbackEntered(): Unit =
      assert(callbackEntered.await(1, TimeUnit.MINUTES), "delivery callback never ran")

    def callbackDuration: Long = callbackReturnedAt.get() - ackAt.get()
    def readyDelay: Long       = readyAt.get() - ackAt.get()
  }

  test("acquiring a reply tracker holds neither the delivery callback nor the traffic behind it") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "isolation-request"
      val replyTopic   = "isolation-reply"
      val otherTopic   = "isolation-other"
      createTopics(bootstrap, requestTopic, replyTopic, otherTopic)

      withPoolAndSender(bootstrap) { (pool, sender, _) =>
        val acquisition = sendAndAcquireInCallback(sender, pool, requestTopic, replyTopic, 60.seconds)

        // Wait until the delivery callback has actually started — from here on the producer's I/O
        // thread is inside the acquisition — then push unrelated traffic through the same producer.
        // A separate test asserted this downstream effect on its own; it is folded in here because it
        // is the same defect observed one step later, and separating them paid a second full
        // assignment stall to re-establish the same premise.
        acquisition.awaitCallbackEntered()
        assert(acquisition.settled.getCount > 0, "assignment completed too quickly to prove anything")

        val sends      = 5
        val confirmed  = new CountDownLatch(sends)
        val sendErrors = new ConcurrentLinkedQueue[Throwable]()

        // Timed from before the first send: a frozen producer I/O thread delays metadata and
        // delivery confirmations alike, and both count against unrelated traffic.
        val startedAt    = System.currentTimeMillis()
        (0 until sends).foreach { i =>
          sender.send(message(otherTopic, s"k-$i", s"v-$i"))(
            _ => confirmed.countDown(),
            e => { sendErrors.add(e); confirmed.countDown() },
          )
        }
        val allConfirmed = confirmed.await(1, TimeUnit.MINUTES)
        val elapsed      = System.currentTimeMillis() - startedAt

        assert(sendErrors.isEmpty, s"unrelated sends failed: ${sendErrors.asScala.map(_.getMessage).mkString(", ")}")
        assert(allConfirmed, "unrelated deliveries were never confirmed")
        assert(
          elapsed < CallbackBudget.toMillis * 2,
          s"unrelated traffic took $elapsed ms to get through while one reply topic was being assigned",
        )

        acquisition.awaitSettled()

        assert(acquisition.failure.get() == null, s"acquisition failed: ${acquisition.failure.get()}")
        assert(
          acquisition.readyDelay > AssignmentStall.toMillis / 2,
          s"assignment was not actually slow (${acquisition.readyDelay} ms): the test proves nothing",
        )
        assert(
          acquisition.callbackDuration < CallbackBudget.toMillis,
          s"delivery callback was held for ${acquisition.callbackDuration} ms waiting for the assignment",
        )
      }
    }
  }

  test("concurrent first use of one reply topic yields a single tracker without holding the callback thread") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "concurrent-request"
      val replyTopic   = "concurrent-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withPoolAndSender(bootstrap) { (pool, sender, _) =>
        val acquisitions =
          (1 to 8).map(_ => sendAndAcquireInCallback(sender, pool, requestTopic, replyTopic, 60.seconds))

        acquisitions.foreach(_.awaitSettled())

        acquisitions.foreach { acquisition =>
          assert(acquisition.failure.get() == null, s"acquisition failed: ${acquisition.failure.get()}")
          assert(
            acquisition.callbackDuration < CallbackBudget.toMillis,
            s"delivery callback was held for ${acquisition.callbackDuration} ms",
          )
        }

        val trackers = acquisitions.map(_.tracker.get()).distinct
        assertEquals(trackers.size, 1, "concurrent acquisitions of one topic must converge on a single tracker")
      }
    }
  }

  test("an acquisition timeout fails only the requesting operation and names the topic") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "timeout-request"
      val replyTopic   = "timeout-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withPoolAndSender(bootstrap) { (pool, sender, _) =>
        // Far shorter than the broker's initial rebalance delay, so the assignment cannot arrive in time.
        val acquisition = sendAndAcquireInCallback(sender, pool, requestTopic, replyTopic, 1.second)
        acquisition.awaitSettled()

        val failure = acquisition.failure.get()
        assert(failure != null, "expected the acquisition to time out")
        assert(
          failure.getMessage.contains(replyTopic) && failure.getMessage.contains("Timed out waiting for consumer assignment"),
          s"unexpected failure message: ${failure.getMessage}",
        )
        assert(
          acquisition.callbackDuration < CallbackBudget.toMillis,
          s"delivery callback was held for ${acquisition.callbackDuration} ms waiting to fail",
        )

        val failedAfter = acquisition.failedAt.get() - acquisition.ackAt.get()
        assert(
          failedAfter < AssignmentStall.toMillis,
          s"failure was reported after $failedAfter ms, later than the requested 1s timeout",
        )
      }
    }
  }

  test("the pool stays usable after an acquisition timeout and the topic can be acquired again") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "retry-request"
      val replyTopic   = "retry-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withPoolAndSender(bootstrap) { (pool, sender, _) =>
        val timedOut = sendAndAcquireInCallback(sender, pool, requestTopic, replyTopic, 1.second)
        timedOut.awaitSettled()
        assert(timedOut.failure.get() != null, "expected the first acquisition to time out")

        // Nothing was poisoned: a later request for the same topic is attempted afresh and succeeds
        // once the broker finishes forming the group.
        val retried = sendAndAcquireInCallback(sender, pool, requestTopic, replyTopic, 60.seconds)
        retried.awaitSettled()

        assert(retried.failure.get() == null, s"retry failed: ${retried.failure.get()}")
        assert(retried.tracker.get() != null, "retry produced no tracker")
        assert(
          retried.callbackDuration < CallbackBudget.toMillis,
          s"delivery callback was held for ${retried.callbackDuration} ms on retry",
        )
      }
    }
  }

  test("the response time a tracker reports excludes subscription setup") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "timing-request"
      val replyTopic   = "timing-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withPoolAndSender(bootstrap) { (pool, sender, statsEngine) =>
        val key        = "timing-key"
        val ackAt      = new AtomicLong(0L)
        val sentAt     = new AtomicLong(0L)
        val registered = new CountDownLatch(1)
        val failure    = new AtomicReference[Throwable](null)
        val next       = new RecordingAction

        sender.send(message(requestTopic, key, "request"))(
          _ => {
            ackAt.set(System.currentTimeMillis())
            pool.acquireTracker(requestTopic, replyTopic, KafkaKeyMatcher, None, 60.seconds)(
              tracker => {
                // Recorded exactly where KafkaRequestReplyAction records it: inside onReady, once
                // tracking is ready — never at ack time.
                val sentTimestamp = System.currentTimeMillis()
                sentAt.set(sentTimestamp)
                tracker ! KafkaMessageTracker.MessagePublished(
                  matchId = key.getBytes,
                  sentTimestamp = sentTimestamp,
                  replyTimeout = 60.seconds.toMillis,
                  checks = Nil,
                  session = Session("scenario", 1L, null),
                  next = next,
                  requestName = "timing-request-reply",
                )
                registered.countDown()
              },
              error => { failure.set(error); registered.countDown() },
            )
          },
          error => { failure.set(error); registered.countDown() },
        )

        assert(registered.await(2, TimeUnit.MINUTES), "tracker never became ready")
        assert(failure.get() == null, s"acquisition failed: ${failure.get()}")

        val setupDuration = sentAt.get() - ackAt.get()
        assert(
          setupDuration > AssignmentStall.toMillis / 2,
          s"subscription setup was not actually slow ($setupDuration ms): the guard proves nothing",
        )

        val replySent = new CountDownLatch(1)
        sender.send(message(replyTopic, key, "reply"))(
          _ => replySent.countDown(),
          error => { failure.set(error); replySent.countDown() },
        )
        assert(replySent.await(30, TimeUnit.SECONDS), "reply was never produced")
        assert(failure.get() == null, s"producing the reply failed: ${failure.get()}")
        assert(next.completed.await(1, TimeUnit.MINUTES), "the tracker never matched the reply")

        val responses = statsEngine.responses.get()
        assertEquals(responses.size, 1, s"expected exactly one logged response, got $responses")
        val reported  = responses.head.endTimestamp - responses.head.startTimestamp
        assertEquals(responses.head.status, OK: Status, s"reply was not reported as OK: ${responses.head}")
        // Fails if the sent timestamp is ever moved back to the ack: the reported duration would
        // then carry the whole subscription setup with it.
        assert(
          reported < AssignmentStall.toMillis / 2,
          s"reported response time of $reported ms includes the $setupDuration ms of subscription setup",
        )
      }
    }
  }

  test("two matchers on one reply topic get independent trackers") {
    withContainers { kafka =>
      val bootstrap    = kafka.bootstrapServers
      val requestTopic = "matcher-isolation-request"
      val replyTopic   = "matcher-isolation-reply"
      createTopics(bootstrap, requestTopic, replyTopic)

      withPoolAndSender(bootstrap) { (pool, _, _) =>
        // Two distinct matcher instances, deliberately not the shared KafkaKeyMatcher singleton:
        // the pool keys trackers by matcher identity, so these must not be conflated.
        val first  = KafkaMessageMatcher(_.key)
        val second = KafkaMessageMatcher(_.key)

        def acquire(matcher: KafkaMatcher): ActorRef[KafkaMessageTracker.TrackerMessage] = {
          val ready   = new AtomicReference[ActorRef[KafkaMessageTracker.TrackerMessage]](null)
          val failure = new AtomicReference[Throwable](null)
          val settled = new CountDownLatch(1)
          pool.acquireTracker(requestTopic, replyTopic, matcher, None, 60.seconds)(
            tracker => { ready.set(tracker); settled.countDown() },
            error => { failure.set(error); settled.countDown() },
          )
          assert(settled.await(2, TimeUnit.MINUTES), "acquisition never completed")
          assert(failure.get() == null, s"acquisition failed: ${failure.get()}")
          ready.get()
        }

        val firstTracker  = acquire(first)
        val secondTracker = acquire(second)

        assert(firstTracker != null && secondTracker != null, "acquisition produced no tracker")
        assert(
          firstTracker ne secondTracker,
          "two distinct matchers on one topic were conflated into a single tracker",
        )
        // Both stay acquirable: neither matcher's registration displaced the other's.
        assert(acquire(first) eq firstTracker, "the first matcher's tracker was replaced")
        assert(acquire(second) eq secondTracker, "the second matcher's tracker was replaced")
      }
    }
  }

  test("a second subscription request does not strand the first one's readiness") {
    withContainers { kafka =>
      val bootstrap  = kafka.bootstrapServers
      val firstTopic = "listener-first"
      val lateTopic  = "listener-second"
      createTopics(bootstrap, firstTopic, lateTopic)

      val consumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
        consumerSettings(bootstrap) + (ConsumerConfig.GROUP_ID_CONFIG -> "listener-replacement"),
        Set.empty,
        _ => (),
        _ => (),
      )
      val thread   = new Thread(consumer, "listener-replacement-consumer")
      thread.setDaemon(true)
      thread.start()

      try {
        val firstReadiness = consumer.requestTopicSubscription(firstTopic)

        // Long enough to land in a later poll cycle, short enough that the broker is still holding
        // back the first assignment: the resulting second subscribe() replaces the rebalance
        // listener, which is what used to drop whatever readiness the previous one was holding.
        Thread.sleep(AssignmentStall.toMillis / 3)

        // Without these the test could pass vacuously: if the consumer thread had not run yet, both
        // requests would be drained by a single updateSubscription, one subscribe would cover both
        // topics, and no listener would ever be replaced. An emptied queue means the first request
        // has already been applied — read reflectively because the wrapped KafkaConsumer is not
        // safe to touch from this thread.
        val queueField = classOf[DynamicKafkaConsumer[_, _]].getDeclaredField("topicsQueue")
        queueField.setAccessible(true)
        assert(
          queueField.get(consumer).asInstanceOf[java.util.Queue[_]].isEmpty,
          "the first subscription was not applied yet, so no second subscribe can replace its listener",
        )
        assert(!firstReadiness.isDone, "the first assignment already landed: nothing is left to strand")

        val lateReadiness = consumer.requestTopicSubscription(lateTopic)

        // Either .get throwing a TimeoutException fails the test — that is the assertion.
        firstReadiness.get(2, TimeUnit.MINUTES)
        lateReadiness.get(2, TimeUnit.MINUTES)
      } finally {
        consumer.close()
        thread.join(5000)
      }
    }
  }
}
