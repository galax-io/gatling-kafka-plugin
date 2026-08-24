package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

import scala.concurrent.duration.DurationInt

/** The five ways a request-reply is supposed to fail, and the counts that prove each one still does.
  *
  * Every KO here is by design, which is why they are together and on their own. They used to be scenarios in
  * [[KafkaGatlingTest]], so a completely healthy run of the positive suite ended with six errors on the console and no way to
  * tell an expected KO from a regression without knowing three issue numbers and two constants. Split by intent, the positive
  * suite prints nothing and this one prints exactly `ExpectedFailures` — ten, as the constants below add up. Keep that sentence
  * and those constants in step: this docstring is the only place the green-run contract is written down.
  *
  * The counts are pinned with `is(n)`, not `lte(n)`, in both directions: a new failure fails the run, and so does an expected
  * one starting to pass. The earlier `lte` bound was satisfied by zero — a reply timeout that silently stopped firing left the
  * run green with its only timeout coverage gone.
  */
class KafkaFailureModesGatlingTest extends Simulation {

  private val bootstrap = "localhost:9093"

  /** Request topic → reply topic for the scenarios the responder serves.
    *
    * `myTopic4` is deliberately absent: `scnRRwo` publishes there and must never be answered, or this simulation loses its only
    * reply-timeout coverage (issue #196). `myTopic2` is present for the opposite reason — see `scnSibling`.
    */
  private val echoRoutes: Map[String, String] = Map(
    "myTopic2" -> "test.t2",
    // Answered with a tombstone rather than an echo (issue #168).
    "myTopic6" -> "test.t6",
    // The two halves of issue #228, deliberately on separate reply topics. The tracker pool keys channels on
    // (reply topic, matcher), so sharing one topic would put a header-correlated tracker in reach of the
    // value-correlated scenario's replies — and since both carry the correlation header, it could match one
    // of them. That is cross-attribution, which is what these scenarios exist to rule out, not reproduce.
    "myTopic7" -> "test.t7",
    "myTopic8" -> "test.t8",
  )

  private val tombstoneRoutes: Set[String] = Set("myTopic6", "myTopic7", "myTopic8")

  /** What each scenario contributes to the two pinned totals, named per scenario rather than folded into one expression.
    *
    * The totals were previously `1 + KeylessKeyUsers + TombstoneUsers` and `ExpectedFailures + TombstoneUsers + 1`, where the
    * bare `1`s were `scnRRwo`'s KO and `scnSibling`'s request. A reader had to reconstruct which scenario each term belonged
    * to, and adding a scenario meant editing an expression whose terms were not attributable. Every scenario below now states
    * its own users, failures and requests, so the totals are a sum of named parts and a new scenario adds a block plus two
    * terms.
    */
  /** `scnRRwo` — one request to a topic nobody answers, one KO on its reply timeout (issue #196). */
  private val ReplyTimeoutUsers    = 1
  private val ReplyTimeoutFailures = ReplyTimeoutUsers
  private val ReplyTimeoutRequests = ReplyTimeoutUsers

  /** `scnSibling` — the traffic `scnRRwo`'s shared reply channel has to keep separate. Succeeds, so it fails nothing. */
  private val SiblingUsers    = 1
  private val SiblingFailures = 0
  private val SiblingRequests = SiblingUsers

  /** `scnRRKeylessKey` — concurrent users with no key to correlate on (issue #167). More than one keeps the expected failure
    * count from being satisfiable by a single lucky request. Each is rejected before it is sent.
    */
  private val KeylessKeyUsers    = 3
  private val KeylessKeyFailures = KeylessKeyUsers
  private val KeylessKeyRequests = KeylessKeyUsers

  /** `scnRRTombstone` — answered with a tombstone (issue #168). Each user contributes one KO on its body check and one
    * follow-up request that must succeed, which is what proves the user was continued rather than stranded.
    */
  private val TombstoneUsers    = 2
  private val TombstoneFailures = TombstoneUsers
  private val TombstoneRequests = TombstoneUsers * 2

  /** `scnRRTombstoneByValue` — issue #228, the half that cannot work. Correlating on the value against a service that answers
    * with tombstones: the reply arrives, carries no value, and can never be matched. Each user contributes one KO, on its reply
    * timeout, and that timeout must now say replies arrived which nothing could correlate.
    */
  private val TombstoneByValueUsers    = 2
  private val TombstoneByValueFailures = TombstoneByValueUsers
  private val TombstoneByValueRequests = TombstoneByValueUsers

  /** `scnRRTombstoneByHeader` — issue #228, the half that does. The same service, correlated on a header it echoes, so every
    * tombstone reaches its request and the body check fails cleanly on the absent payload instead of the request timing out.
    * One KO per user, and it is a check failure rather than a timeout — which is the whole distinction.
    */
  private val TombstoneByHeaderUsers    = 2
  private val TombstoneByHeaderFailures = TombstoneByHeaderUsers
  private val TombstoneByHeaderRequests = TombstoneByHeaderUsers

  /** The reply budget for the two #228 scenarios, and the boundary both assertions are read against.
    *
    * Long enough to outlast establishing a reply channel, short enough that the value-correlated scenario's timeouts do not
    * dominate the run. `scnRRTombstoneByHeader` must land far below it and `scnRRTombstoneByValue` must reach it — that gap is
    * how the harness tells "answered but uncorrelatable" from "answered and correlated" without being able to assert on failure
    * text.
    */
  private val TombstoneCorrelationTimeout = 12.seconds

  private val ExpectedFailures =
    ReplyTimeoutFailures + SiblingFailures + KeylessKeyFailures + TombstoneFailures +
      TombstoneByValueFailures + TombstoneByHeaderFailures

  /** Every request this simulation issues.
    *
    * Pinned for the same reason the failure counts are: a scenario that stops running produces no KO either, and the failure
    * total alone would go green on a suite that had quietly lost one.
    */
  private val ExpectedRequests =
    ReplyTimeoutRequests + SiblingRequests + KeylessKeyRequests + TombstoneRequests +
      TombstoneByValueRequests + TombstoneByHeaderRequests

  private val responder = new EchoResponder(bootstrap, "kafka-failure-modes-responder", echoRoutes, tombstoneRoutes)

  before(responder.start())
  after(responder.close())

  val kafkaProtocolRRString: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .withDefaultTimeout

  // The reply timeout doubles as the acquisition timeout, so it has to outlast establishing the reply
  // channel. At a previous 1 second `scnRRwo` failed with "Timed out waiting for consumer assignment to
  // topic 'test.t2'" — a KO for the right count but the wrong reason, and a timing-dependent one at that.
  // It is supposed to prove that a request nobody answers times out waiting for its reply, which is what
  // it now does: myTopic4 has no responder, so nothing ever publishes tstBytesWO.
  val kafkaProtocolRRBytes2: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .timeout(15.seconds)
    .matchByValue

  /** Issue #228, the half that cannot work: correlation on the record value against a service that answers with tombstones.
    *
    * A tombstone carries no value, so `matchByValue` can derive nothing from the reply and it can never reach its request. The
    * request therefore times out — and the point of the scenario is that the timeout must no longer read as though nothing
    * answered.
    */
  val kafkaProtocolTombstoneByValue: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> bootstrap,
    )
    .timeout(TombstoneCorrelationTimeout)
    .matchByValue

  /** Issue #228, the half that does: correlation on a header the service echoes, which a tombstone still carries.
    *
    * This is the shape the Migration Guide names as required for services that may answer with tombstones, and running it is
    * what makes that recommendation more than prose.
    */
  val kafkaProtocolTombstoneByHeader: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> bootstrap,
    )
    .timeout(TombstoneCorrelationTimeout)
    .matchByMessage(EchoResponder.correlationIdFromHeader)

  /** Reply timeout (issue #196). Requests `myTopic4`, which the responder does not consume, so no reply can ever arrive.
    *
    * `myTopic4` rather than `myTopic2`: the responder serves `myTopic2`, so sharing it would answer this request too and
    * destroy the only reply-timeout coverage there is. The reply topic stays `test.t2`, which `scnSibling` shares — so this
    * request also has to survive a reply arriving on its own channel for somebody else.
    */
  val scnRRwo: ScenarioBuilder = scenario("RequestReply w/o answer")
    .exec(
      kafka("Request Reply Bytes wo").requestReply
        .requestTopic("myTopic4")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("testWO".getBytes(), "tstBytesWO".getBytes()),
    )

  /** The sibling traffic `scnRRwo`'s reply channel has to keep separate from its own pending request.
    *
    * The tracker pool keys channels on (reply topic, matcher), so this scenario and `scnRRwo` share one `test.t2` channel under
    * `matchByValue`: two records pending at once, and exactly one reply arriving for them. That reply carries this scenario's
    * payload, so it must resolve this request and leave `scnRRwo`'s alone — crediting the wrong one is the cross-attribution
    * failure the matcher exists to prevent (issue #167).
    *
    * In the combined simulation this arrived by accident, from a positive scenario that happened to use the same reply topic
    * under the same matcher. Splitting the suite took that scenario away, so the traffic is produced deliberately here rather
    * than being quietly lost.
    *
    * A request-reply rather than a bare produce, and that is not a stylistic choice. A produce-only echo lands on `test.t2`
    * about two seconds in, which is *before* the shared channel finishes acquiring its assignment — the acquisition this
    * protocol's 15-second timeout exists to outlast — so the channel never sees it and the coverage is imaginary. Measured, not
    * assumed: with the produce-only version carrying `tstBytesWO`, the broker shows the echo on `test.t2` and `scnRRwo` times
    * out anyway. Registering a request is what guarantees the channel is up before its reply can exist.
    */
  val scnSibling: ScenarioBuilder = scenario("Reply channel sibling")
    .exec(
      kafka("Reply Channel Sibling").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("sibling".getBytes(), "tstBytesSibling".getBytes()),
    )

  /** Keyless under the default key matching (issue #167): there is nothing to correlate a reply on, so each request must be
    * failed at issue time rather than sent. Before the fix these all registered under one shared empty id, and a reply resolved
    * whichever request happened to hold it.
    *
    * `Option(null)` is `None`, so a null key expression is how the DSL expresses "this request carries no key". The failure
    * *count* is a smoke check, not the gate: a half-reverted fix that published these requests would produce the same count via
    * displacement plus one timeout. That the requests never reach the broker is asserted in `KeylessCorrelationSpec`, which can
    * observe the request topic directly.
    */
  val scnRRKeylessKey: ScenarioBuilder = scenario("RequestReply keyless by key")
    .exec(
      kafka("Request Reply Keyless Key").requestReply
        .requestTopic("myTopic5")
        .replyTopic("test.t5")
        .send[String, String](null, "no-key-to-match-on"),
    )

  /** Issue #168. The reply is a tombstone, so the body check has nothing to check against.
    *
    * Two requests, and the second is the point. A stranded virtual user produces *no* KO at all, so a failure-count assertion
    * on its own goes green on exactly the run this scenario exists to catch. "Tombstone Follow Up" only executes if the user
    * survived the first request, which is what turns a hang into a visible, countable difference.
    */
  val scnRRTombstone: ScenarioBuilder = scenario("RequestReply tombstone reply")
    // A distinct key per user, from a feeder rather than an EL reference to `userId`: `userId` is not a
    // session attribute, so `#{userId}` fails to resolve and the request dies at build time — reported
    // as a crash with no request stats at all, which makes the scenario silently assert nothing. The
    // keys must differ because matchByKey would otherwise displace one request with the other.
    .feed(Iterator.from(1).map(i => Map("tombKey" -> s"tomb-$i")): Feeder[String])
    .exec(
      kafka("Request Reply Tombstone").requestReply
        .requestTopic("myTopic6")
        .replyTopic("test.t6")
        .send[String, String]("#{tombKey}", "body")
        // `is("body")`, not a value that never matches: a check failing on *every* reply cannot tell a tombstone from a
        // normal echo, so the scenario would stay green with tombstoneRoutes empty or the route lookup broken. This one
        // succeeds on an echo and can only KO when the payload is absent.
        .check(bodyString.is("body")),
    )
    .exec(
      kafka("Tombstone Follow Up")
        .topic("myTopic3")
        .send[String]("survived"),
    )

  /** Issue #228 — the reply arrives and cannot be placed, so the request times out having been answered.
    *
    * Distinct payloads per user, from a feeder: `matchByValue` correlates on the value, so two users sending the same body
    * would share one correlation id and displace each other — the collision issue #167 fixed for keys, one field over. The
    * failure this scenario expects has to be the timeout, not a displacement.
    *
    * **What the assertions below do and do not prove.** Gatling's assertion API cannot read failure text, so the pair with
    * `scnRRTombstoneByHeader` proves only the half that is expressible: these requests span the whole reply budget and the
    * header-correlated ones cannot, which establishes that header correlation reaches the request and value correlation does
    * not. It does **not** prove that the timeout names the uncorrelatable replies — these same assertions pass with the
    * tracker's diagnosis removed entirely. That half is covered against a real broker by
    * `org.galaxio.gatling.kafka.integration.UncorrelatableReplyReportingSpec`, which can read the reported message, and the
    * wording is pinned in `KafkaMessageTrackerSpec`.
    */
  val scnRRTombstoneByValue: ScenarioBuilder = scenario("RequestReply tombstone by value")
    .feed(Iterator.from(1).map(i => Map("tombValue" -> s"tomb-by-value-$i")): Feeder[String])
    .exec(
      kafka("Request Reply Tombstone By Value").requestReply
        .requestTopic("myTopic7")
        .replyTopic("test.t7")
        .send[String, String]("#{tombValue}", "#{tombValue}"),
    )

  /** Issue #228 — the same service, correlated on a header it echoes, so every tombstone reaches its request.
    *
    * The check is what proves the reply arrived at the request rather than merely arriving. `bodyString.is("body")` succeeds on
    * an echo and can only fail on an absent payload, so a KO here is v1.2.0's clean absent-payload failure — the outcome that
    * was structurally unreachable on the value-correlated path, because the reply never reached its request at all.
    */
  val scnRRTombstoneByHeader: ScenarioBuilder = scenario("RequestReply tombstone by header")
    .feed(Iterator.from(1).map(i => Map("corrId" -> s"corr-$i")): Feeder[String])
    .exec(
      kafka("Request Reply Tombstone By Header").requestReply
        .requestTopic("myTopic8")
        .replyTopic("test.t8")
        .send[String, String](
          "#{corrId}",
          "body",
          session =>
            session("corrId")
              .validate[String]
              .map(id => new RecordHeaders().add(EchoResponder.CorrelationHeader, id.getBytes): Headers),
        )
        .check(bodyString.is("body")),
    )

  setUp(
    scnRRwo.inject(atOnceUsers(ReplyTimeoutUsers)).protocols(kafkaProtocolRRBytes2),
    scnSibling.inject(atOnceUsers(SiblingUsers)).protocols(kafkaProtocolRRBytes2),
    scnRRKeylessKey.inject(atOnceUsers(KeylessKeyUsers)).protocols(kafkaProtocolRRString),
    scnRRTombstone.inject(atOnceUsers(TombstoneUsers)).protocols(kafkaProtocolRRString),
    scnRRTombstoneByValue.inject(atOnceUsers(TombstoneByValueUsers)).protocols(kafkaProtocolTombstoneByValue),
    scnRRTombstoneByHeader.inject(atOnceUsers(TombstoneByHeaderUsers)).protocols(kafkaProtocolTombstoneByHeader),
  ).assertions(
    // Five sources of expected failure, expected for five different reasons:
    //
    //   - scnRRwo sends to myTopic4, which the responder does not consume, so it always KOs on its reply
    //     timeout. That is the reply-timeout coverage (issue #196).
    //   - scnRRKeylessKey supplies no key under key matching, so there is nothing to correlate a reply on
    //     and every one of its requests is failed at issue time without being sent (issue #167).
    //   - scnRRTombstone is answered with a null payload, so its body check has nothing to check against
    //     — and the user must be continued rather than stranded (issue #168).
    //   - scnRRTombstoneByValue correlates on the value against a tombstone-answering service, so its
    //     replies can never be placed and every request KOs on its reply timeout (issue #228).
    //   - scnRRTombstoneByHeader correlates the same replies on a header, so each one reaches its request
    //     and KOs on the body check instead — the pair is what separates "answered uncorrelatably" from
    //     "not answered" (issue #228).
    global.failedRequests.count.is(ExpectedFailures),
    global.allRequests.count.is(ExpectedRequests),
    details("Request Reply Bytes wo").failedRequests.count.is(ReplyTimeoutFailures),
    // Issue #227. The rejection reports under a name of its own, because a request that never reached the
    // broker must not put a sample into the percentile of the request the simulation declared — Gatling
    // feeds a name's response-time digest from every entry carrying it, KO included, and its assertion API
    // has no successful-only scope for response time.
    //
    // The pair with `global.allRequests` above is what proves the samples *moved* rather than being
    // duplicated: a run reporting each rejection under both names would still satisfy this line, and would
    // push the pinned request total from ExpectedRequests to ExpectedRequests + KeylessKeyRequests.
    //
    // The declared name is deliberately not asserted to be zero: no request is reported under it at all any
    // more, so `details("Request Reply Keyless Key")` resolves to nothing and such an assertion would fail
    // as unresolvable rather than pass.
    details("Request Reply Keyless Key [rejected: no correlation id]").failedRequests.count.is(KeylessKeyFailures),
    details("Request Reply Tombstone").failedRequests.count.is(TombstoneFailures),
    // Issue #168, and the pair is the assertion. The KO count alone would be satisfied by a run in which
    // the users hung — a stranded user reports nothing — so the follow-up success count is what proves
    // they were continued rather than lost.
    details("Tombstone Follow Up").successfulRequests.count.is(TombstoneUsers),
    // Not decoration, and the pair with "Request Reply Bytes wo" is the assertion: this reply and that
    // pending request sit on one shared channel, so a run that credits them the wrong way round shows up
    // as this going KO and that going OK. If it stops being sent, the shared-channel coverage is gone and
    // the run would still be green.
    details("Reply Channel Sibling").successfulRequests.count.is(SiblingUsers),
    // Issue #228, and the two lines are one assertion. Same service, same tombstones, two correlation
    // strategies — the pair is what separates "nobody answered" from "somebody answered in a shape this
    // configuration cannot correlate", which used to be the same reported outcome.
    //
    // Response time is the only way to say it here: Gatling's assertion API cannot read failure text, but it
    // can tell a request that spent its whole reply budget from one that did not. The value-correlated
    // requests can only end on the timeout; the header-correlated ones can only end on their body check,
    // which happens as soon as the reply lands.
    //
    // Read the limit honestly: this pair covers correlation, not diagnosis. Nothing here observes the
    // timeout's wording, so removing the tracker's uncorrelatable-reply clause leaves this simulation
    // green. That clause is covered end to end by UncorrelatableReplyReportingSpec, which drives a real
    // broker and reads the reported message, and its wording is pinned in KafkaMessageTrackerSpec.
    details("Request Reply Tombstone By Value").failedRequests.count.is(TombstoneByValueFailures),
    details("Request Reply Tombstone By Value").responseTime.min.gte(TombstoneCorrelationTimeout.toMillis.toInt),
    details("Request Reply Tombstone By Header").failedRequests.count.is(TombstoneByHeaderFailures),
    details("Request Reply Tombstone By Header").responseTime.max.lt(TombstoneCorrelationTimeout.toMillis.toInt / 2),
  ).maxDuration(180.seconds)

}
