package org.galaxio.gatling.kafka.client

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.actor.{Actor, Behavior, Cancellable}
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.client.KafkaMessageTracker._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.{KafkaCheck, KafkaLogging}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object KafkaMessageTracker {

  def actor[K, V](
      actorName: String,
      statsEngine: StatsEngine,
      clock: Clock,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): Actor[TrackerMessage] =
    new KafkaMessageTracker[K, V](actorName, statsEngine, clock, messageMatcher, responseTransformer)

  sealed trait TrackerMessage

  /** @param sentTimestamp
    *   when the record was handed to the producer, and what this request is measured from.
    * @param token
    *   distinguishes this registration from any other that shares its `matchId`.
    *
    * The handoff, not the request's start and not the broker's acknowledgement. Starting before channel acquisition would
    * deduct the whole assignment wait from the reply budget, so a request could be reported as timed out milliseconds after
    * being sent. Starting at the acknowledgement — as this did until it was simplified away — excluded the produce leg from
    * every reported time, understating what the virtual user actually waits for.
    *
    * The match id is not an identity. With the default key matcher it *is* the message key, so a feeder cycling a fixed key set
    * — or any request that sets no key at all, which serialises to the same empty id — reuses it constantly. Without a token, a
    * [[SendFailed]] for one request lands on whichever record happens to hold the key at the time and fails it with an
    * unrelated error.
    */
  final case class MessagePublished(
      matchId: Array[Byte],
      sentTimestamp: Long,
      replyTimeout: Long,
      checks: List[KafkaCheck],
      session: Session,
      next: Action,
      requestName: String,
      onComplete: () => Unit = () => (),
      token: Long = 0L,
  ) extends TrackerMessage

  final case class MessageConsumed(
      received: Long,
      message: KafkaProtocolMessage,
  ) extends TrackerMessage

  /** The producer failed to deliver the request identified by `matchId`/`token`.
    *
    * The request was registered before the send, so a delivery failure has to un-register it — otherwise the record sits until
    * its reply timeout and, worse, its channel never returns to idle because nothing releases the reference acquisition took.
    *
    * `errorType` is the failure's own kind — `TimeoutException`, `RecordTooLargeException`, `NotLeaderOrFollowerException` —
    * taken from the exception the producer raised. It lands in the response-code slot, which is what a report groups and
    * filters failures by. The path used to put `"500"` there, an HTTP status with no meaning in Kafka.
    */
  final case class SendFailed(
      matchId: Array[Byte],
      errorMessage: String,
      token: Long = 0L,
      errorType: Option[String] = None,
  ) extends TrackerMessage

  final case class ConsumerFailure(errorMessage: String) extends TrackerMessage

  /** Asks the tracker to cancel its periodic timeout scan and stop accepting work, giving `reason` to anything that arrives
    * afterwards.
    *
    * Sent by [[KafkaMessageTrackerPool]] once the reply channel this tracker belongs to has been released. It exists because
    * Gatling's `ActorSystem` offers no way to stop an actor from outside — `die` is an `Effect` reachable only from the actor's
    * own behaviour — so stopping has to be asked for (issue #166).
    *
    * The tracker does not simply `die`. A registration can still be in flight from another thread when this arrives — the pool
    * checks for consumer failure before handing a tracker over, and the consumer can fail in the gap — and `die` drops whatever
    * follows, so that virtual user would get no success, no failure and no continuation at all. `reason` is what it is failed
    * with instead.
    */
  private[client] final case class Stop(reason: String) extends TrackerMessage

  /** Package-visible so a spec can drive one scan deterministically, rather than waiting on the scheduler. */
  private[client] final case object TimeoutScan extends TrackerMessage

  /** Map key for a match id, comparing the bytes themselves.
    *
    * The alternative was Base64-encoding the id to get a `String` key, which allocated an encoder result on every publish,
    * every acknowledgement, every delivery failure and every reply — three or four times per request on the tracker's single
    * thread — purely to obtain something with value equality. `Array[Byte]` has reference equality, hence the wrapper.
    */
  private final class MatchKey(val bytes: Array[Byte]) {
    override val hashCode: Int               = java.util.Arrays.hashCode(bytes)
    override def equals(other: Any): Boolean = other match {
      case that: MatchKey => java.util.Arrays.equals(bytes, that.bytes)
      case _              => false
    }
  }

  private def matchKeyFor(m: Array[Byte]): MatchKey =
    new MatchKey(if (m == null) Array.emptyByteArray else m)
}

/** Actor to record request and response Kafka Events, publishing data to the Gatling core DataWriter
  */
class KafkaMessageTracker[K, V](
    name: String,
    statsEngine: StatsEngine,
    clock: Clock,
    messageMatcher: KafkaMatcher,
    responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
) extends Actor[TrackerMessage](name) with KafkaLogging {

  private val sentMessages     = mutable.HashMap.empty[MatchKey, MessagePublished]
  private val timedOutMessages = mutable.ArrayBuffer.empty[MessagePublished]

  /** The periodic timeout scan, once something with a reply timeout has been published.
    *
    * Retained rather than discarded so [[KafkaMessageTracker.Stop]] can cancel it. The handle held nothing before, which meant
    * the scan outlived every channel that ever armed one: it captures `self`, so the tracker — and with it the stats engine,
    * the clock and the matcher closures — stayed reachable from the actor system's scheduler, and that scheduler is a single
    * thread shared by the whole simulation, so each leaked scan also cost one wakeup per second for the rest of the run (issue
    * #166).
    */
  private var periodicTimeoutScan: Option[Cancellable] = None

  private def triggerPeriodicTimeoutScan(): Unit =
    if (periodicTimeoutScan.isEmpty) {
      periodicTimeoutScan = Some(scheduler.scheduleAtFixedRate(1000.millis) {
        self ! TimeoutScan
      })
    }

  /** Reports a matched reply, whether or not the acknowledgement has landed yet. The caller has already removed the record. */
  private def completeMatched(
      published: MessagePublished,
      receivedTimestamp: Long,
      message: KafkaProtocolMessage,
  ): Unit =
    try
      processMessage(
        published.session,
        published.sentTimestamp,
        receivedTimestamp,
        published.checks,
        message,
        published.next,
        published.requestName,
      )
    finally published.onComplete()

  override def init(): Behavior[TrackerMessage] = {
    // The request is registered here, before it is handed to the producer, so a reply cannot be looked
    // up before the record for it exists (issue #191). The acknowledgement timestamp is not known yet
    case messageSent: MessagePublished =>
      val key = matchKeyFor(messageSent.matchId)
      if (logger.underlying.isDebugEnabled) {
        logger.debug("Published with MatchId: {}", describeBytes(messageSent.matchId))
      }
      // A match id already in flight would otherwise be overwritten silently, and the displaced request
      // would never be completed by any path: no success, no failure, no continuation, and its channel
      // reference never released. Fail it explicitly instead — losing a request to a reused correlation
      // id is a defect in the simulation, and it has to be visible as one.
      sentMessages.remove(key).foreach { displaced =>
        failPending(
          displaced,
          clock.nowMillis,
          None,
          s"Match id reused while a request was still in flight on it (${describeBytes(messageSent.matchId)}); " +
            "give each request-reply a distinct key, or match on a field that is unique per request",
        )
      }
      sentMessages += key -> messageSent
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
      }
      stay

    case SendFailed(matchId, errorMessage, token, errorType) =>
      val key = matchKeyFor(matchId)
      // Same token check: without it a late delivery failure removes and fails whichever request now
      // holds the key, reporting it with an unrelated error while the request that actually failed is
      // never completed at all.
      sentMessages.get(key).filter(_.token == token) match {
        case Some(pending) =>
          sentMessages.remove(key)
          logger.error("Delivery failed for {}: {}", describeBytes(matchId), errorMessage)
          failPending(pending, clock.nowMillis, errorType, errorMessage)
        case None          =>
          // The request is already gone — almost always because its reply timeout is shorter than the
          // producer's delivery timeout, so it was reported as unanswered long before delivery gave up.
          // The stats entry cannot be amended, so say plainly in the log that the earlier timeout for
          // this request was really a delivery failure; otherwise a broker outage reads as a slow
          // system under test.
          logger.warn(
            "Delivery failed for {} after its request had already been reported: {}. " +
              "The reply timeout is shorter than the producer's delivery.timeout.ms, so this request was " +
              "reported as a reply timeout rather than as this failure.",
            describeBytes(matchId),
            errorMessage,
          )
      }
      stay

    // message was received; publish stats and remove from the map
    case MessageConsumed(receivedTimestamp, forTransformMessage) =>
      val message = responseTransformer.map(_(forTransformMessage)).getOrElse(forTransformMessage)
      val replyId = messageMatcher.responseMatch(message)
      if (replyId == null) {
        logger.error("no messageMatcher key for read message {}", message.key)
      } else {
        if (message.key == null || message.value == null) {
          logger.warn(" --- received message with null key or value")
        }
        // Every one of these renders the whole payload, one String per character, and SLF4J's
        // placeholder form defers formatting but not argument evaluation. Guarded so a reply costs
        // nothing to log when the level is off — this runs on the thread that gates reply throughput.
        if (logger.underlying.isTraceEnabled) {
          logger.trace(" --- received key={} value={}", describeBytes(message.key), describeBytes(message.value))
        }
        if (logger.underlying.isDebugEnabled) {
          logMessage(s"Record received key=${describeBytes(message.key)}", message)
          logger.debug(
            "Received with MatchId: {}, producerTopic: {}, consumerTopic: {}",
            describeBytes(replyId),
            message.producerTopic,
            message.consumerTopic,
          )
        }
        // Reported as soon as it arrives, whether or not the acknowledgement has landed. A reply that
        // matches nothing — one for a request already completed or timed out, a duplicate, or
        // third-party traffic on a held channel — stays silent, as it always has.
        //
        // One map operation: remove returns the record, where get followed by a remove inside the
        // completion hashed the key twice for no observable difference on a single-threaded actor.
        sentMessages.remove(matchKeyFor(replyId)).foreach(completeMatched(_, receivedTimestamp, message))
      }
      stay

    case ConsumerFailure(errorMessage) =>
      val now     = clock.nowMillis
      logger.error("Consumer failure propagated to tracker: {}", errorMessage)
      val pending = sentMessages.values.toList
      sentMessages.clear()
      pending.foreach(failPending(_, now, None, s"Consumer failure: $errorMessage"))
      stay

    case TimeoutScan =>
      val now = clock.nowMillis
      sentMessages.valuesIterator.foreach { p =>
        val replyTimeout = p.replyTimeout
        if (replyTimeout > 0 && (now - p.sentTimestamp) > replyTimeout) {
          timedOutMessages += p
        }
      }
      // Cleared in a finally: the buffer is actor state, and a throw out of the loop would otherwise
      // leave it populated for the next scan to replay — reporting each stale entry a second time and
      // releasing its channel a second time, which drives the reference count negative and lets the
      // idle sweep tear down a channel that still has live requests on it.
      try
        for (p <- timedOutMessages) {
          logger.warn("Did not receive match for {} after {}ms", describeBytes(p.matchId), p.replyTimeout)
          sentMessages.remove(matchKeyFor(p.matchId))
          failPending(p, now, None, s"Reply timeout after ${p.replyTimeout} ms")
        }
      finally timedOutMessages.clear()
      stay

    case Stop(reason) =>
      // Cancelling matters as much as stopping: the scan captures `self`, and Gatling's scheduler is one
      // thread shared by the whole simulation, so a scan left running keeps firing at a tracker nobody
      // holds any more and keeps it reachable — the leak would survive almost intact (issue #166).
      periodicTimeoutScan.foreach(_.cancel())
      periodicTimeoutScan = None
      // Anything still pending belongs to a channel that is going away; fail it here rather than leaving
      // it for a scan that has just been cancelled.
      val now     = clock.nowMillis
      val pending = sentMessages.values.toList
      sentMessages.clear()
      pending.foreach(failPending(_, now, None, reason))
      become(stopped(reason))
  }

  /** Behaviour after [[Stop]]: the channel is gone, but a registration may still be in flight from another thread.
    *
    * Gatling's `die` would drop it, and the virtual user behind it would get no success, no failure and no continuation — it
    * would simply hang for the rest of the run. Failing it costs one stats entry and keeps the "exactly one outcome per
    * request" contract that the rest of this actor maintains.
    */
  private def stopped(reason: String): Behavior[TrackerMessage] = {
    case messageSent: MessagePublished =>
      failPending(messageSent, clock.nowMillis, None, reason)
      stay

    case other =>
      logger.debug("Dropping {} on a released tracker", other.getClass.getSimpleName)
      stay
  }

  /** Reports one pending request as failed and releases the channel reference its acquisition took.
    *
    * `onComplete` runs in a `finally` because it is what returns the reference: skipping it on a reporting error would leave
    * the channel permanently in use, never idle and never reclaimed.
    */
  private def failPending(
      published: MessagePublished,
      now: Long,
      responseCode: Option[String],
      message: String,
  ): Unit =
    try
      executeNext(
        published.session.markAsFailed,
        published.sentTimestamp,
        now,
        KO,
        published.next,
        published.requestName,
        responseCode,
        Some(message),
      )
    finally published.onComplete()

  private def executeNext(
      session: Session,
      sentTimestamp: Long,
      receivedTimestamp: Long,
      status: Status,
      next: Action,
      requestName: String,
      responseCode: Option[String],
      message: Option[String],
  ): Unit = {
    statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sentTimestamp,
      receivedTimestamp,
      status,
      responseCode,
      message,
    )
    next ! session.logGroupRequestTimings(sentTimestamp, receivedTimestamp)
  }

  /** Processes a matched message
    */
  private def processMessage(
      session: Session,
      sentTimestamp: Long,
      receivedTimestamp: Long,
      checks: List[KafkaCheck],
      message: KafkaProtocolMessage,
      next: Action,
      requestName: String,
  ): Unit = {
    val (newSession, error) = Check.check(message, session, checks)
    error match {
      case Some(Failure(errorMessage)) =>
        executeNext(
          newSession.markAsFailed,
          sentTimestamp,
          receivedTimestamp,
          KO,
          next,
          requestName,
          message.responseCode,
          Some(errorMessage),
        )
      case _                           =>
        executeNext(newSession, sentTimestamp, receivedTimestamp, OK, next, requestName, None, None)
    }
  }
}
