package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

import scala.concurrent.duration.DurationInt

/** The three ways a request-reply is supposed to fail, and the counts that prove each one still does.
  *
  * Every KO here is by design, which is why they are together and on their own. They used to be scenarios in
  * [[KafkaGatlingTest]], so a completely healthy run of the positive suite ended with six errors on the console and no way to
  * tell an expected KO from a regression without knowing three issue numbers and two constants. Split by intent, the positive
  * suite prints nothing and this one prints exactly six.
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
  )

  private val tombstoneRoutes: Set[String] = Set("myTopic6")

  /** Concurrent users with no key to correlate on (issue #167). More than one keeps the expected failure count from being
    * satisfiable by a single lucky request.
    */
  private val KeylessKeyUsers = 3

  /** Virtual users answered with a tombstone (issue #168). Each contributes one expected KO and one follow-up success. */
  private val TombstoneUsers = 2

  private val ExpectedFailures = 1 + KeylessKeyUsers + TombstoneUsers

  /** Every request this simulation issues: the expected failures, one follow-up per tombstone user, and the sibling request.
    *
    * Pinned for the same reason the failure counts are: a scenario that stops running produces no KO either, and the failure
    * total alone would go green on a suite that had quietly lost one.
    */
  private val ExpectedRequests = ExpectedFailures + TombstoneUsers + 1

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

  setUp(
    scnRRwo.inject(atOnceUsers(1)).protocols(kafkaProtocolRRBytes2),
    scnSibling.inject(atOnceUsers(1)).protocols(kafkaProtocolRRBytes2),
    scnRRKeylessKey.inject(atOnceUsers(KeylessKeyUsers)).protocols(kafkaProtocolRRString),
    scnRRTombstone.inject(atOnceUsers(TombstoneUsers)).protocols(kafkaProtocolRRString),
  ).assertions(
    // Three sources of expected failure, expected for three different reasons:
    //
    //   - scnRRwo sends to myTopic4, which the responder does not consume, so it always KOs on its reply
    //     timeout. That is the reply-timeout coverage (issue #196).
    //   - scnRRKeylessKey supplies no key under key matching, so there is nothing to correlate a reply on
    //     and every one of its requests is failed at issue time without being sent (issue #167).
    //   - scnRRTombstone is answered with a null payload, so its body check has nothing to check against
    //     — and the user must be continued rather than stranded (issue #168).
    global.failedRequests.count.is(ExpectedFailures),
    global.allRequests.count.is(ExpectedRequests),
    details("Request Reply Bytes wo").failedRequests.count.is(1),
    details("Request Reply Keyless Key").failedRequests.count.is(KeylessKeyUsers),
    details("Request Reply Tombstone").failedRequests.count.is(TombstoneUsers),
    // Issue #168, and the pair is the assertion. The KO count alone would be satisfied by a run in which
    // the users hung — a stranded user reports nothing — so the follow-up success count is what proves
    // they were continued rather than lost.
    details("Tombstone Follow Up").successfulRequests.count.is(TombstoneUsers),
    // Not decoration, and the pair with "Request Reply Bytes wo" is the assertion: this reply and that
    // pending request sit on one shared channel, so a run that credits them the wrong way round shows up
    // as this going KO and that going OK. If it stops being sent, the shared-channel coverage is gone and
    // the run would still be green.
    details("Reply Channel Sibling").successfulRequests.count.is(1),
  ).maxDuration(120.seconds)

}
