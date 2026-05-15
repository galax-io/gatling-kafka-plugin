package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.util
import java.util.concurrent.{CompletableFuture, CountDownLatch, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

class KafkaSenderSpec extends AnyFunSuite {

  private val protocolMessage = KafkaProtocolMessage(
    key = "k".getBytes(),
    value = "v".getBytes(),
    inputTopic = "test.in",
    outputTopic = "test.out",
  )

  test("uses the callback-based producer send overload") {
    val producer = new TrackingProducer
    val sender   = KafkaSender(producer)

    sender.send(protocolMessage)(_ => (), _ => ())

    assert(producer.callbackSendCount == 1)
    assert(producer.plainSendCount == 0)
  }

  test("invokes producer send on the caller thread") {
    val producer         = new TrackingProducer
    val sender           = KafkaSender(producer)
    val callingThreadRef = new AtomicReference[String]()

    callingThreadRef.set(Thread.currentThread().getName)
    sender.send(protocolMessage)(_ => (), _ => ())

    assert(producer.callbackThreadName.contains(callingThreadRef.get()))
  }

  test("forwards successful producer callbacks") {
    val producer      = new TrackingProducer
    val sender        = KafkaSender(producer)
    val successLatch  = new CountDownLatch(1)
    val failureCalled = new AtomicReference[Throwable]()

    sender.send(protocolMessage)(
      _ => successLatch.countDown(),
      failure => failureCalled.set(failure),
    )

    producer.completeSuccessfully()

    assert(successLatch.await(1, TimeUnit.SECONDS))
    assert(failureCalled.get() == null)
  }

  test("forwards failed producer callbacks") {
    val producer      = new TrackingProducer
    val sender        = KafkaSender(producer)
    val successCalled = new AtomicReference[RecordMetadata]()
    val failureRef    = new AtomicReference[Throwable]()
    val failureLatch  = new CountDownLatch(1)
    val boom          = new RuntimeException("boom")

    sender.send(protocolMessage)(
      metadata => successCalled.set(metadata),
      failure => {
        failureRef.set(failure)
        failureLatch.countDown()
      },
    )

    producer.completeExceptionally(boom)

    assert(failureLatch.await(1, TimeUnit.SECONDS))
    assert(successCalled.get() == null)
    assert(failureRef.get() eq boom)
  }

  private final class TrackingProducer extends Producer[Array[Byte], Array[Byte]] {
    private val callbackRef   = new AtomicReference[Callback]()
    private val callbackCount = new AtomicReference[Int](0)
    private val plainCount    = new AtomicReference[Int](0)
    private val threadNameRef = new AtomicReference[Option[String]](None)

    def callbackSendCount: Int             = callbackCount.get()
    def plainSendCount: Int                = plainCount.get()
    def callbackThreadName: Option[String] = threadNameRef.get()

    override def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Future[RecordMetadata] = {
      plainCount.set(plainCount.get() + 1)
      CompletableFuture.completedFuture(null.asInstanceOf[RecordMetadata])
    }

    override def send(record: ProducerRecord[Array[Byte], Array[Byte]], callback: Callback): Future[RecordMetadata] = {
      callbackCount.set(callbackCount.get() + 1)
      threadNameRef.set(Option(Thread.currentThread().getName))
      callbackRef.set(callback)
      CompletableFuture.completedFuture(null.asInstanceOf[RecordMetadata])
    }

    def completeSuccessfully(): Unit =
      Option(callbackRef.get()).foreach(_.onCompletion(null, null))

    def completeExceptionally(exception: Exception): Unit =
      Option(callbackRef.get()).foreach(_.onCompletion(null, exception))

    override def flush(): Unit                                                                                = ()
    override def partitionsFor(topic: String): util.List[org.apache.kafka.common.PartitionInfo]               = util.Collections.emptyList()
    override def metrics(): util.Map[org.apache.kafka.common.MetricName, _ <: org.apache.kafka.common.Metric] =
      util.Collections.emptyMap()
    override def clientInstanceId(timeout: Duration): org.apache.kafka.common.Uuid                            =
      throw new UnsupportedOperationException("not used in tests")
    override def close(): Unit                                                                                = ()
    override def close(timeout: Duration): Unit                                                               = ()
    override def initTransactions(): Unit                                                                     = ()
    override def beginTransaction(): Unit                                                                     = ()
    override def sendOffsetsToTransaction(
        offsets: util.Map[org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata],
        consumerGroupId: String,
    ): Unit                                                                                                   = ()
    override def sendOffsetsToTransaction(
        offsets: util.Map[org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata],
        groupMetadata: org.apache.kafka.clients.consumer.ConsumerGroupMetadata,
    ): Unit                                                                                                   = ()
    override def commitTransaction(): Unit                                                                    = ()
    override def abortTransaction(): Unit                                                                     = ()
  }
}
