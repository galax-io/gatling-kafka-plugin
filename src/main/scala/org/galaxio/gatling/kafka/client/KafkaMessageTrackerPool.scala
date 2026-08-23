package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.{ActorRef, ActorSystem}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.galaxio.gatling.kafka.KafkaLogging
import org.galaxio.gatling.kafka.client.KafkaMessageTracker.MessageConsumed
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.{KafkaProtocolMessage, KafkaSerdesImplicits}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{
  ConcurrentHashMap,
  ExecutorService,
  Executors,
  ScheduledThreadPoolExecutor,
  ThreadFactory,
  TimeUnit,
}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object KafkaMessageTrackerPool {

  def apply(
      consumerSettings: Map[String, AnyRef],
      actorSystem: ActorSystem,
      statsEngine: StatsEngine,
      clock: Clock,
  ): Option[KafkaMessageTrackerPool] =
    Option.when(consumerSettings.contains(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))(
      new KafkaMessageTrackerPool(consumerSettings, actorSystem, statsEngine, clock),
    )

  /** How long simulation shutdown waits for queued acquisition continuations to report their outcome. */
  private[client] val setupDrainTimeoutMillis: Long = 5000L

  /** How often idle reply channels are checked for release. */
  private[client] val idleSweepIntervalMillis: Long = 1000L

  /** Default idle grace before a reply channel is released.
    *
    * Deliberately not derived from the protocol's reply timeout: that would make the grace shorter than a single channel
    * establishment whenever the timeout is short, so a scenario would churn exactly as issue #165 describes. This is a
    * think-time scale, not a request-latency scale — it needs to outlast the gaps a realistic profile puts between requests
    * (pacing, pauses, ramp) and nothing else.
    */
  private[kafka] val defaultIdleGraceMillis: Long = 30000L
}

final class KafkaMessageTrackerPool(
    consumerSettings: Map[String, AnyRef],
    actorSystem: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaLogging with NameGen with KafkaSerdesImplicits {

  /** How long a reply channel may sit with nothing in flight before it is released.
    *
    * Not a constructor parameter: adding one would change this class's primary constructor signature, which is a binary break
    * for anything compiled against the current release and would force a major version for what is a bug fix. Internal to the
    * plugin and writable only so integration tests can shorten it — production never assigns it.
    */
  @volatile private[kafka] var idleGraceMillis: Long = KafkaMessageTrackerPool.defaultIdleGraceMillis

  /** A reply channel and its bookkeeping.
    *
    * `refCount` is the number of requests currently in flight on this `(consumerTopic, matcher)`. It is an accurate measure of
    * that, and an inaccurate predictor of future use — which is the distinction issue #165 turns on. Releasing the channel the
    * moment the count reaches zero reads "nothing in flight" as "never needed again"; in a sequential scenario that inference
    * is wrong after every single request, so the channel is destroyed milliseconds before it is needed again and every request
    * pays a fresh subscription and rebalance.
    *
    * So reaching zero no longer tears the channel down — it starts an idle clock. `idleSweep` releases a channel only once it
    * has had nothing in flight for `graceMillis`. A sequential scenario re-acquires long before that and keeps its channel; a
    * reply topic derived per virtual user goes idle and is reclaimed, which is what issue #78 requires.
    */
  private case class TrackerEntry(
      actor: ActorRef[KafkaMessageTracker.TrackerMessage],
      refCount: AtomicInteger,
      /** `clock.nowMillis` when `refCount` last reached zero. Only meaningful while `refCount <= 0`. */
      idleSince: AtomicLong,
  )

  // Uses reference equality so distinct matcher instances with the same identityHashCode
  // are never conflated (identityHashCode collisions only affect bucket distribution).
  private class MatcherRef(val matcher: KafkaMatcher) {
    override def hashCode(): Int           = System.identityHashCode(matcher)
    override def equals(obj: Any): Boolean = obj.isInstanceOf[MatcherRef] && (matcher eq obj.asInstanceOf[MatcherRef].matcher)
    override def toString: String          = s"${matcher.getClass.getSimpleName}@${System.identityHashCode(matcher)}"
  }

  // consumerTopic -> (MatcherRef -> TrackerEntry)
  private val trackers        = new ConcurrentHashMap[String, ConcurrentHashMap[MatcherRef, TrackerEntry]]
  private val trackerName     = "kafkaTracker"
  private val consumerFailure = new AtomicReference[Exception](null)

  // Per-instance executor so shutdown of one pool doesn't affect other pools or subsequent simulations.
  private val consumerExecutor: ExecutorService = Executors.newSingleThreadExecutor()

  private def daemonThreads(name: String): ThreadFactory =
    (runnable: Runnable) => {
      val thread = new Thread(runnable, name)
      thread.setDaemon(true)
      thread
    }

  /** Owns only scheduled work: the per-acquisition timeouts and the idle sweep. Nothing that can block runs here.
    *
    * Single-threaded, so anything long-running on it delays everything else on it — and what it schedules are the deadlines
    * that bound exactly such waits. A continuation blocking here would stall the timeouts meant to fire on it.
    */
  private val setupExecutor: ScheduledThreadPoolExecutor = {
    val executor = new ScheduledThreadPoolExecutor(1, daemonThreads("gatling-kafka-tracker-setup"))
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /** Runs the readiness continuations, which hand the record to the producer.
    *
    * `KafkaProducer.send` blocks up to `max.block.ms` resolving metadata or waiting for accumulator space. On the scheduler
    * above, one such block would stall every other virtual user's acquisition, the idle sweep, and the acquisition timeouts
    * themselves — so a request that should fail on schedule would report late, which is the tool distorting its own
    * measurement. Cached rather than fixed: the threads are short-lived and mostly blocked, and a bounded pool would only move
    * the head-of-line blocking rather than remove it.
    */
  private val continuationExecutor: ExecutorService =
    Executors.newCachedThreadPool(daemonThreads("gatling-kafka-tracker-continuation"))

  private val consumer: DynamicKafkaConsumer[Array[Byte], Array[Byte]] =
    DynamicKafkaConsumer(
      if (consumerSettings.contains(ConsumerConfig.GROUP_ID_CONFIG))
        consumerSettings
      else
        consumerSettings + (ConsumerConfig.GROUP_ID_CONFIG -> s"gatling-kafka-test-${java.util.UUID.randomUUID()}"),
      Set.empty,
      record => {
        val kafkaProtocolMessage = KafkaProtocolMessage.from(record, None)
        val receivedTimestamp    = clock.nowMillis
        // Lock-free read: a stale snapshot after concurrent removal is benign —
        // the tracker actor simply won't match any pending request.
        Option(trackers.get(record.topic())).foreach { innerMap =>
          innerMap.values().forEach { entry =>
            entry.actor ! MessageConsumed(
              receivedTimestamp,
              kafkaProtocolMessage,
            )
          }
        }
      },
      exception => {
        logger.error(
          "Consumer failure detected, propagating to {} active tracker(s): {}",
          trackers.size(),
          exception.getMessage,
        )
        logger.debug("Consumer failure stacktrace", exception)
        if (consumerFailure.compareAndSet(null, exception)) {
          val failure = KafkaMessageTracker.ConsumerFailure(exception.getMessage)
          trackers
            .values()
            .forEach(_.values().forEach { entry =>
              entry.actor ! failure
              // Then stop it. Nothing will ever release these entries — the pool is finished — so their
              // timeout scans would otherwise keep firing for the rest of the run against bookkeeping the
              // failure broadcast has just cleared. The mailbox is FIFO, so the failure above is processed
              // first and every pending request still gets its KO.
              entry.actor ! KafkaMessageTracker.Stop(s"Consumer failure: ${exception.getMessage}")
            })
          trackers.clear()
        }
      },
    )

  // One sweep per pool, not a timer per tracker. Off both the producer callback and the consumer poll
  // thread, so a release never sits on a path that measures latency.
  private val idleSweep = setupExecutor.scheduleAtFixedRate(
    () =>
      try sweepIdleTrackers()
      catch { case NonFatal(e) => logger.error("Idle tracker sweep failed", e) },
    KafkaMessageTrackerPool.idleSweepIntervalMillis,
    KafkaMessageTrackerPool.idleSweepIntervalMillis,
    TimeUnit.MILLISECONDS,
  )

  private val consumerFuture = consumerExecutor.submit(consumer)
  actorSystem.registerOnTermination {
    // Anything still held at the end of the run is released here rather than left to the actor system's
    // scheduler shutting down underneath it. Runs before ActorSystem.close() shuts its executor, so the
    // messages are still dispatched.
    trackers.values().forEach(_.values().forEach(_.actor ! KafkaMessageTracker.Stop("Simulation is shutting down")))
    trackers.clear()
    logger.debug("Closing consumer {}", consumer)
    consumer.close()
    try {
      consumerFuture.get()
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
    }
    // Ordering is deliberate, not belt-and-braces: this runs well before `setupExecutor.shutdown()`
    // below, with a consumer-future wait and a continuation drain in between. Without the cancel the
    // periodic sweep keeps firing across that window, on trackers already told to stop.
    idleSweep.cancel(false)
    consumerExecutor.shutdown()
    // Closing the consumer above failed every pending readiness, and those continuations run on the
    // continuation executor: they still have to report that failure back to the waiting virtual users,
    // so they are drained rather than discarded.
    continuationExecutor.shutdown()
    if (!continuationExecutor.awaitTermination(KafkaMessageTrackerPool.setupDrainTimeoutMillis, TimeUnit.MILLISECONDS)) {
      logger.warn("Tracker continuation executor did not drain in time; forcing shutdown")
      continuationExecutor.shutdownNow()
    }
    // The scheduler holds only acquisition timeouts, which are scheduled up to a full reply timeout out
    // and are moot now, plus the cancelled sweep. Dropping delayed tasks stops them stalling termination
    // until the grace period below expires; at the JDK default of true they would be held instead.
    setupExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    setupExecutor.shutdown()
    if (!setupExecutor.awaitTermination(KafkaMessageTrackerPool.setupDrainTimeoutMillis, TimeUnit.MILLISECONDS)) {
      logger.warn("Tracker setup executor did not drain in time; forcing shutdown")
      setupExecutor.shutdownNow()
    }
  }

  private def withProducerTopic(producerTopic: String): KafkaProtocolMessage => KafkaProtocolMessage =
    _.copy(producerTopic = producerTopic)

  private def consumerFailedException(failure: Exception): IllegalStateException =
    new IllegalStateException("Kafka consumer failed; tracker pool can no longer be used", failure)

  /** Obtains, creating it if needed, the tracker for `(consumerTopic, messageMatcher)` and hands it to `onReady`. Never blocks
    * the calling thread: when the reply topic has to be subscribed first, the wait for its assignment happens on the pool's
    * setup executor and `onReady` runs there once the assignment lands. Exactly one of `onReady` and `onFailure` is invoked,
    * once.
    */
  def acquireTracker(
      producerTopic: String,
      consumerTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      timeout: FiniteDuration,
  )(
      onReady: ActorRef[KafkaMessageTracker.TrackerMessage] => Unit,
      onFailure: Throwable => Unit,
  ): Unit = {
    val failure = consumerFailure.get()
    if (failure != null) {
      onFailure(consumerFailedException(failure))
      return
    }

    val mRef = new MatcherRef(messageMatcher)

    // Fast path: outer computeIfPresent holds the bin lock while we touch the inner
    // map, so a concurrent releaseTracker cannot remove the outer entry in between.
    var found: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    trackers.computeIfPresent(
      consumerTopic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          mRef,
          (_, entry) => {
            entry.refCount.incrementAndGet()
            found = entry.actor
            entry
          },
        )
        innerMap
      },
    )
    if (found != null) {
      // The consumer can fail between the check above and this point, in which case the failure
      // broadcast has already flushed this tracker and cleared the map. Handing it over anyway would
      // leave the request to wait out its whole reply timeout instead of failing with the real cause
      // — the same guard the readiness continuation applies on the slow path.
      val late = consumerFailure.get()
      if (late != null) onFailure(consumerFailedException(late))
      else deliverReady(onReady, onFailure, found)
      return
    }

    // Slow path needs both executors. Check up front: once either is shut down a rejected continuation
    // would be absorbed by the CompletableFuture that schedules it and never surface, leaving the
    // caller with no outcome at all.
    if (setupExecutor.isShutdown || continuationExecutor.isShutdown) {
      onFailure(
        new IllegalStateException(
          s"Tracker pool is shutting down; cannot start tracking topic '$consumerTopic'",
        ),
      )
      return
    }

    // Slow path: request the subscription and let the readiness future carry the wait.
    logger.debug(
      "Creating new tracker for topic {} matcher {}, there are currently {} other topic entries",
      consumerTopic,
      mRef,
      trackers.size(),
    )
    try {
      val readiness   = consumer.requestTopicSubscription(consumerTopic)
      val timeoutTask = setupExecutor.schedule(
        new Runnable {
          override def run(): Unit = {
            readiness.completeExceptionally(
              new RuntimeException(s"Timed out waiting for consumer assignment to topic '$consumerTopic' after $timeout"),
            )
          }
        },
        timeout.toMillis,
        TimeUnit.MILLISECONDS,
      )

      readiness.whenCompleteAsync(
        (_: Void, error: Throwable) => {
          timeoutTask.cancel(false)
          // `error` arrives unwrapped: `readiness` is the plain CompletableFuture returned by
          // requestTopicSubscription, every completion path calls completeExceptionally with a raw
          // exception, and this callback is registered on that same future rather than a derived stage.
          // A CompletionException unwrap used to sit here and could never fire.
          if (error != null) onFailure(error)
          else {
            // The consumer can have failed between the assignment landing and this continuation
            // running; registering now would hand back a tracker that already missed the failure
            // broadcast, and the caller would wait out its whole reply timeout instead.
            val late = consumerFailure.get()
            if (late != null) onFailure(consumerFailedException(late))
            else {
              var tracker: ActorRef[KafkaMessageTracker.TrackerMessage] = null
              try
                tracker = registerTracker(producerTopic, consumerTopic, messageMatcher, responseTransformer, mRef)
              catch { case NonFatal(e) => onFailure(e) }
              if (tracker != null) deliverReady(onReady, onFailure, tracker)
            }
          }
        },
        continuationExecutor,
      )
    } catch {
      // The pool is being torn down: the setup executor rejects new work once it is shut down.
      // Report it, rather than letting it escape into the producer's delivery callback where it
      // would be swallowed and leave the virtual user with neither a success nor a failure.
      case NonFatal(e) => onFailure(e)
    }
  }

  /** Hands the tracker over, keeping a failure in `onReady` from escaping into the caller — which may be the producer's I/O
    * callback, where it would be swallowed and leave the virtual user with no outcome at all.
    *
    * Such a failure is reported through `onFailure`. Handing over means telling the tracker actor, which enqueues the message
    * before dispatching it, so in principle a request could be reported as failed and still be processed later. In practice the
    * dispatch only rejects once the actor system's executor is shut down, which is terminal — the enqueued message can never
    * run — so reporting the failure is right, and staying silent would drop the virtual user from the report entirely.
    */
  private def deliverReady(
      onReady: ActorRef[KafkaMessageTracker.TrackerMessage] => Unit,
      onFailure: Throwable => Unit,
      tracker: ActorRef[KafkaMessageTracker.TrackerMessage],
  ): Unit =
    try onReady(tracker)
    catch {
      case NonFatal(e) =>
        logger.error("Tracker was acquired but handing it to the caller failed", e)
        onFailure(e)
    }

  private def registerTracker(
      producerTopic: String,
      consumerTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      mRef: MatcherRef,
  ): ActorRef[KafkaMessageTracker.TrackerMessage] = {
    val transformations =
      responseTransformer.fold(withProducerTopic(producerTopic))(_.compose(withProducerTopic(producerTopic)))

    // Atomic insert-or-increment under the outer bin lock so a concurrent
    // sweep cannot remove the outer entry between get-or-create and insert.
    var result: ActorRef[KafkaMessageTracker.TrackerMessage] = null
    trackers.compute(
      consumerTopic,
      (_, existing) => {
        val innerMap = if (existing != null) existing else new ConcurrentHashMap[MatcherRef, TrackerEntry]()
        innerMap.compute(
          mRef,
          (_, entry) => {
            if (entry != null) {
              entry.refCount.incrementAndGet()
              result = entry.actor
              entry
            } else {
              // Started here rather than before the compute: acquisition no longer blocks, so N
              // concurrent first uses of a topic all reach this point, and every loser of the race
              // would otherwise leave behind a started actor that nothing ever stops — each one
              // still running its own periodic timeout scan. Creating the actor does not touch
              // `trackers`, so it is safe under the bin lock.
              val newActor = actorSystem.actorOf(
                KafkaMessageTracker.actor(
                  genName(trackerName),
                  statsEngine,
                  clock,
                  messageMatcher,
                  Option(transformations),
                ),
              )
              result = newActor
              TrackerEntry(newActor, new AtomicInteger(1), new AtomicLong(clock.nowMillis))
            }
          },
        )
        innerMap
      },
    )
    result
  }

  /** Records that one request has finished with this channel.
    *
    * Reaching zero starts the idle clock; it does not release anything. See `TrackerEntry` for why the two are not the same
    * event. Releasing happens in `sweepIdleTrackers`, off the request path entirely.
    */
  def releaseTracker(consumerTopic: String, messageMatcher: KafkaMatcher): Unit = {
    val mRef = new MatcherRef(messageMatcher)
    trackers.computeIfPresent(
      consumerTopic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          mRef,
          (_, entry) => {
            if (entry.refCount.decrementAndGet() <= 0) entry.idleSince.set(clock.nowMillis)
            entry
          },
        )
        innerMap
      },
    )
  }

  /** Releases channels that have had nothing in flight for their grace period, and unsubscribes any reply topic left with no
    * channels at all.
    *
    * Runs on the setup executor, so neither the producer's I/O callback thread nor the consumer's poll thread ever performs a
    * release. Every removal re-checks the entry's state inside `computeIfPresent`, so a request acquiring concurrently either
    * increments first (and the entry survives) or finds the entry already gone (and takes the slow path) — never both.
    */
  private[client] def sweepIdleTrackers(): Unit = {
    val now = clock.nowMillis
    trackers.forEach { (consumerTopic, innerMap) =>
      innerMap.forEach { (mRef, entry) =>
        if (entry.refCount.get() <= 0 && now - entry.idleSince.get() >= idleGraceMillis) {
          var released: ActorRef[KafkaMessageTracker.TrackerMessage] = null
          innerMap.computeIfPresent(
            mRef,
            (_, current) =>
              if (current.refCount.get() <= 0 && now - current.idleSince.get() >= idleGraceMillis) {
                logger.debug("Releasing idle tracker for topic {} matcher {}", consumerTopic, mRef)
                released = current.actor
                null
              } else current,
          )
          // Outside the compute lambda, following removeTopicSubscription below: the bin lock is held
          // inside it, and telling an actor is a side effect on another object. Sent only once the entry
          // is gone, so no acquisition can be handed a tracker that is about to die. Dropping the entry
          // without this is the whole of issue #166 — the registration went, everything hanging off it
          // stayed.
          if (released != null) released ! KafkaMessageTracker.Stop("Reply channel was released after going idle")
        }
      }
      if (innerMap.isEmpty) {
        var doUnsubscribe = false
        trackers.computeIfPresent(
          consumerTopic,
          (_, current) =>
            if (current.isEmpty) {
              doUnsubscribe = true
              null
            } else current,
        )
        if (doUnsubscribe) consumer.removeTopicSubscription(consumerTopic)
      }
    }
  }
}
