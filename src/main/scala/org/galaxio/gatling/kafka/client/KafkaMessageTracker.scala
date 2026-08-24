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
import org.galaxio.gatling.kafka.actions.KafkaRequestFailureMessages
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.{KafkaCheck, KafkaLogging}

import scala.collection.mutable
import scala.util.control.NonFatal
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
    * reuses it constantly — as does any extractor that falls back to a constant. Without a token, a [[SendFailed]] for one
    * request lands on whichever record happens to hold the key at the time and fails it with an unrelated error.
    *
    * A request that supplies *no* id never gets here: [[org.galaxio.gatling.kafka.actions.KafkaRequestReplyAction]] fails it
    * before registration, because there is nothing to correlate a reply on (issue #167).
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
    * `errorMessage` carries the failure's own kind as well as its text — `TimeoutException: Expiring 1 record(s)`, composed by
    * `KafkaRequestFailureMessages.failureCause`. The kind used to travel beside it in `errorType`, which was handed to
    * Gatling's response-code slot — a slot no OSS report reads (issue #254).
    *
    * `errorType` is kept, ignored, rather than deleted: this is a published case class, so dropping a field would change
    * `apply`, `copy`, `unapply` and the accessor and break anything compiled against 2.0.x at run time. Removal belongs in the
    * next major release, after this deprecation cycle. It carries no `@deprecated` annotation because a case-class parameter
    * propagates the warning into its own synthesized `apply`/`copy`, which `-Xfatal-warnings` rejects and which the build
    * forbids suppressing with `@nowarn`.
    *
    * @deprecated
    *   nothing reads `errorType`; put the failure's kind in `errorMessage` via `KafkaRequestFailureMessages.failureCause`
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
    *
    * An absent id and an empty one are different keys. `java.util.Arrays` gives that for free — `hashCode(null)` is 0 against 1
    * for an empty array, and `equals(null, Array.emptyByteArray)` is false — so no branch is needed here, and none is on the
    * per-reply path.
    */
  private final class MatchKey(val bytes: Array[Byte]) {
    override val hashCode: Int               = java.util.Arrays.hashCode(bytes)
    override def equals(other: Any): Boolean = other match {
      case that: MatchKey => java.util.Arrays.equals(bytes, that.bytes)
      case _              => false
    }
  }

  /** No substitution. This used to fold `null` into `Array.emptyByteArray`, which made "this request has no id" and "this
    * request's id is empty" the same map key: every keyless request-reply shared one slot, and a reply carrying an empty key
    * resolved whichever request currently held it (issue #167).
    *
    * What removing it buys is exactly one thing — an absent id and an empty one no longer collide. It does **not** make the
    * table injective: `java.util.Arrays.equals(null, null)` is `true`, so two null ids would still alias each other. Nothing
    * here prevents that, which is why a null id is refused at both edges instead — `MessagePublished` will not register one and
    * `MessageConsumed` will not look one up. Those two guards are the invariant; this function only stops widening it.
    */
  private def matchKeyFor(m: Array[Byte]): MatchKey = new MatchKey(m)
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

  /** Whether this tracker has seen a reply the configured matcher could not read a correlation id from.
    *
    * A flag, not a count, and the scope is narrower than it looks — both deliberate, and both corrections of what this said
    * first:
    *
    *   - **Not a count, because the count reached the report.** Gatling keys its error table on the exact message text, so a
    *     growing number in the message turned one row reading `Reply timeout after N ms ×500` into five hundred rows of one,
    *     degrading the reader-facing signal this exists to sharpen. The number is worth having in the log, where it costs
    *     nothing, and is worth nothing in the message.
    *   - **Not "this channel", and not "ever".** The pool delivers every record on a reply *topic* to every matcher registered
    *     for it, so a reply another scenario correlates perfectly still lands here and sets this flag. And the flag dies with
    *     the tracker: the idle sweep releases a channel that has been quiet for its grace period, and re-acquisition builds a
    *     new actor starting from `false`. So this answers "has *a* reply reached this topic during this tracker's life that
    *     *this* matcher could not read", which is why the message below says topic rather than channel and offers the finding
    *     as a possibility rather than a diagnosis.
    *
    * Why it exists at all: a service answering with a tombstone under value matching produces replies with no value, so there
    * is nothing to derive an id from. The record was dropped with a log line and the request behind it failed on its reply
    * timeout, indistinguishable from a system under test that never answered (issue #228). The reply still cannot be attributed
    * to any request — doing that is the cross-attribution failure issue #167 exists to prevent — so this qualifies the timeout
    * it causes, and nothing else.
    *
    * Plain `var`s: actor state on a single-threaded actor, so no synchronisation. `Long` on the count because the workload this
    * targets is exactly the one that would overflow an `Int`.
    */
  private var sawUncorrelatableReply: Boolean = false

  /** How many, for the log only. Never reaches a report — see [[sawUncorrelatableReply]]. */
  private var uncorrelatableReplyCount: Long = 0L

  private def triggerPeriodicTimeoutScan(): Unit =
    if (periodicTimeoutScan.isEmpty) {
      periodicTimeoutScan = Some(scheduler.scheduleAtFixedRate(1000.millis) {
        self ! TimeoutScan
      })
    }

  /** Reports a matched reply, whether or not the acknowledgement has landed yet. The caller has already removed the record.
    *
    * The `try` covers **only** check evaluation, not the reporting that follows it. That distinction is the whole safety of
    * this method: `executeNext` logs the response *and* advances the virtual user, so wrapping it as well meant a throw after
    * the response had already been dispatched would run it a second time — two stats entries for one request and the same user
    * pushed down the chain twice, in an actor built around exactly one terminal outcome.
    *
    * Catching around the check is what issue #168 needs. A check that throws used to escape into a `try`/`finally` with nothing
    * to catch it: the record had already been removed from `sentMessages`, so no timeout scan could fail it either, and
    * `next ! session` was never reached — the virtual user simply stopped, with nothing in the report. The preparers no longer
    * throw on a tombstone, but the guarantee worth having is "no check can strand a virtual user", and that has to hold for
    * check types this plugin does not own.
    */
  private def completeMatched(
      published: MessagePublished,
      receivedTimestamp: Long,
      message: KafkaProtocolMessage,
  ): Unit =
    try {
      val outcome =
        try Right(Check.check(message, published.session, published.checks))
        catch {
          case NonFatal(e) =>
            logger.error(s"Check execution failed for ${published.requestName}; reporting it as a failure", e)
            Left(s"Check execution failed: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")
        }

      outcome match {
        case Left(errorMessage)                    =>
          executeNext(
            published.session.markAsFailed,
            published.sentTimestamp,
            receivedTimestamp,
            KO,
            published.next,
            published.requestName,
            Some(errorMessage),
          )
        case Right((newSession, Some(Failure(m)))) =>
          executeNext(
            newSession.markAsFailed,
            published.sentTimestamp,
            receivedTimestamp,
            KO,
            published.next,
            published.requestName,
            Some(m),
          )
        case Right((newSession, _))                =>
          executeNext(
            newSession,
            published.sentTimestamp,
            receivedTimestamp,
            OK,
            published.next,
            published.requestName,
            None,
          )
      }
    } finally published.onComplete()

  override def init(): Behavior[TrackerMessage] = {
    // The request is registered here, before it is handed to the producer, so a reply cannot be looked
    // up before the record for it exists (issue #191). The acknowledgement timestamp is not known yet
    case messageSent: MessagePublished if messageSent.matchId == null =>
      // The table cannot hold this safely: `Arrays.equals(null, null)` is true, so two of these would
      // alias each other and re-create issue #167 one key over. The sending side rejects such a request
      // before it gets here, so this is the symmetric guard to the one `MessageConsumed` already has —
      // reject at both edges rather than trusting a caller a layer away.
      logger.error("Refusing to register a request with no match id; it could not be correlated to any reply")
      failPending(
        messageSent,
        clock.nowMillis,
        "Cannot correlate a reply: this request was registered with no match id",
        Some(KafkaRequestFailureMessages.RejectionKind.NoCorrelationId),
      )
      stay

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
          s"Match id reused while a request was still in flight on it (${describeBytes(messageSent.matchId)}); " +
            "give each request-reply a distinct key, or match on a field that is unique per request",
        )
      }
      sentMessages += key -> messageSent
      if (messageSent.replyTimeout > 0) {
        triggerPeriodicTimeoutScan()
      }
      stay

    case SendFailed(matchId, errorMessage, token, _) =>
      val key = matchKeyFor(matchId)
      // Same token check: without it a late delivery failure removes and fails whichever request now
      // holds the key, reporting it with an unrelated error while the request that actually failed is
      // never completed at all.
      sentMessages.get(key).filter(_.token == token) match {
        case Some(pending) =>
          sentMessages.remove(key)
          logger.error("Delivery failed for {}: {}", describeBytes(matchId), errorMessage)
          failPending(pending, clock.nowMillis, errorMessage, Some(KafkaRequestFailureMessages.RejectionKind.NotDelivered))
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
        // A reply arrived and cannot be placed. Recorded here so the timeout it causes can say so, rather
        // than leaving the run to claim the system under test never answered (issue #228).
        sawUncorrelatableReply = true
        uncorrelatableReplyCount += 1L
        // Guarded, and at WARN. For the service issue #228 targets — one answering every request with a
        // tombstone under value matching — this is not an edge case but the whole steady state, so an
        // unguarded line here is one ERROR and one `describeBytes` per reply on the thread that gates reply
        // throughput: the run's own logging becomes the bottleneck it is trying to diagnose. The rule is
        // the one this file already states for the trace and debug siblings below — SLF4J defers
        // formatting but not argument evaluation.
        //
        // Reports the count and both fields. Naming only the key was misleading on the path that reaches
        // here most: under value matching the key is present and fine, and the value is what is absent.
        if (logger.underlying.isWarnEnabled) {
          logger.warn(
            "No correlation id in reply #{}: {} could not read one from key={} value={}",
            uncorrelatableReplyCount.toString,
            messageMatcher.getClass.getSimpleName.stripSuffix("$"),
            describeBytes(message.key),
            describeBytes(message.value),
          )
        }
      } else {
        // Only the value. An absent key stopped being suspicious when the plugin started publishing one —
        // a keyless request-reply correlating on the value or a header is a supported shape and the
        // migration guide recommends it, so warning per reply would put a line on the thread that gates
        // reply throughput for every message of a normal run (issue #167). A null value still deserves a
        // mention: it is what breaks body checks (issue #168).
        if (message.value == null && logger.underlying.isDebugEnabled) {
          logger.debug(" --- received message with null value (tombstone)")
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
      pending.foreach(failPending(_, now, s"Consumer failure: $errorMessage"))
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
          failPending(p, now, replyTimeoutMessage(p.replyTimeout))
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
      pending.foreach(failPending(_, now, reason))
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
      failPending(messageSent, clock.nowMillis, reason)
      stay

    case other =>
      logger.debug("Dropping {} on a released tracker", other.getClass.getSimpleName)
      stay
  }

  /** What a reply timeout says, qualified by whether this channel ever received a reply it could not read.
    *
    * With nothing uncorrelatable behind it the wording is exactly what it has always been, so a run against a genuinely
    * unresponsive service reads as it did. With something behind it the timeout is still reported — it did happen — but the
    * reader is told that replies arrived which the matcher could not place, because "nobody answered" and "somebody answered in
    * a shape this configuration cannot correlate" are different findings and used to be the same line (issue #228).
    *
    * Only the timeout path. A request failed by a delivery failure, a consumer failure, a channel stop or a failing check
    * already has a definite cause, and appending a second speculative one to it would make the report worse.
    */
  private def replyTimeoutMessage(replyTimeout: Long): String = {
    val timedOut = s"Reply timeout after $replyTimeout ms"
    if (!sawUncorrelatableReply) timedOut
    else
      // Two properties this sentence has to keep, both learned the hard way:
      //
      //   - It carries no number. Gatling's error table is keyed by the message text, so a per-timeout
      //     count fragments one row into hundreds. The count is in the log instead.
      //   - The remedy is derived from the matcher, never hardcoded. Any matcher can return null — a
      //     keyless reply under matchByKey, a missing header under matchByMessage — so advice fixed at
      //     "correlate on a key or a header" was handed to readers already doing exactly that. This is
      //     the defect `KafkaRequestFailureMessages.remedyFor` was written to eliminate one layer over.
      s"$timedOut. Replies also arrived on this reply topic that " +
        s"${KafkaRequestFailureMessages.matcherName(messageMatcher)} could not read a correlation id from, so this request " +
        "may have been answered in a shape this configuration cannot correlate rather than not answered at all. " +
        KafkaRequestFailureMessages.remedyFor(messageMatcher)
  }

  /** Reports one pending request as failed and releases the channel reference its acquisition took.
    *
    * `onComplete` runs in a `finally` because it is what returns the reference: skipping it on a reporting error would leave
    * the channel permanently in use, never idle and never reclaimed.
    */
  private def failPending(
      published: MessagePublished,
      now: Long,
      message: String,
      rejection: Option[KafkaRequestFailureMessages.RejectionKind] = None,
  ): Unit =
    try
      executeNext(
        published.session.markAsFailed,
        published.sentTimestamp,
        now,
        KO,
        published.next,
        // A rejection reports under a name of its own, for the reason the action's own rejections do: no
        // record reached the broker, so the interval measures nothing about the system under test and must
        // not join the declared name's response-time digest (issue #227). Delivery failure is the largest
        // instance of that — at Kafka's default `delivery.timeout.ms` a broker outage puts a two-minute
        // sample on every in-flight request — and it was the one left behind when the action-side twins
        // moved.
        rejection.fold(published.requestName)(KafkaRequestFailureMessages.rejectedRequestName(published.requestName, _)),
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
      message: Option[String],
  ): Unit = {
    statsEngine.logResponse(
      session.scenario,
      session.groups,
      requestName,
      sentTimestamp,
      receivedTimestamp,
      status,
      // Gatling's response-code slot. Every OSS writer drops it — the file serializer writes groups,
      // name, timestamps, status and message and nothing more, and the record the report parses back
      // has no field for it — so a failure's kind goes in the message, where it is actually shown
      // (issue #254). Do not repopulate this without first checking that a report can display it.
      None,
      message,
    )
    next ! session.logGroupRequestTimings(sentTimestamp, receivedTimestamp)
  }
}
