package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, CyclicBarrier}

/** Tests the ConcurrentHashMap refcount algorithm used by KafkaMessageTrackerPool. Exercises the same computeIfPresent/compute
  * pattern against concurrent acquire+release, including the two-level map used for matcher isolation.
  */
class TrackerRefCountSpec extends munit.FunSuite {

  private case class Entry(value: String, refCount: AtomicInteger)

  // Identity-based key mirroring KafkaMessageTrackerPool.MatcherRef
  private class IdentityKey(val id: AnyRef) {
    override def hashCode(): Int           = System.identityHashCode(id)
    override def equals(obj: Any): Boolean = obj.isInstanceOf[IdentityKey] && (id eq obj.asInstanceOf[IdentityKey].id)
  }

  // --- Single-level helpers (original algorithm) ---

  private def acquire(map: ConcurrentHashMap[String, Entry], key: String, newValue: String): String = {
    var found: String = null
    map.computeIfPresent(
      key,
      (_, entry) => {
        entry.refCount.incrementAndGet()
        found = entry.value
        entry
      },
    )
    if (found != null) return found

    var result: String = null
    map.compute(
      key,
      (_, existing) => {
        if (existing != null) {
          existing.refCount.incrementAndGet()
          result = existing.value
          existing
        } else {
          result = newValue
          Entry(newValue, new AtomicInteger(1))
        }
      },
    )
    result
  }

  private def release(map: ConcurrentHashMap[String, Entry], key: String): Boolean = {
    var removed = false
    map.computeIfPresent(
      key,
      (_, entry) => {
        if (entry.refCount.decrementAndGet() <= 0) {
          removed = true
          null
        } else entry
      },
    )
    removed
  }

  // --- Two-level helpers (mirrors KafkaMessageTrackerPool with MatcherRef) ---

  private type InnerMap = ConcurrentHashMap[IdentityKey, Entry]
  private type OuterMap = ConcurrentHashMap[String, InnerMap]

  private def acquire2(map: OuterMap, topic: String, matcherRef: IdentityKey, newValue: String): String = {
    var found: String = null
    map.computeIfPresent(
      topic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          matcherRef,
          (_, entry) => {
            entry.refCount.incrementAndGet()
            found = entry.value
            entry
          },
        )
        innerMap
      },
    )
    if (found != null) return found

    var result: String = null
    map.compute(
      topic,
      (_, existing) => {
        val innerMap = if (existing != null) existing else new ConcurrentHashMap[IdentityKey, Entry]()
        innerMap.compute(
          matcherRef,
          (_, entry) => {
            if (entry != null) {
              entry.refCount.incrementAndGet()
              result = entry.value
              entry
            } else {
              result = newValue
              Entry(newValue, new AtomicInteger(1))
            }
          },
        )
        innerMap
      },
    )
    result
  }

  private def release2(map: OuterMap, topic: String, matcherRef: IdentityKey): Boolean = {
    var topicRemoved = false
    map.computeIfPresent(
      topic,
      (_, innerMap) => {
        innerMap.computeIfPresent(
          matcherRef,
          (_, entry) => {
            if (entry.refCount.decrementAndGet() <= 0) null
            else entry
          },
        )
        if (innerMap.isEmpty) { topicRemoved = true; null }
        else innerMap
      },
    )
    topicRemoved
  }

  // ==================== Single-level tests ====================

  test("acquire increments refcount, release decrements to zero and removes") {
    val map = new ConcurrentHashMap[String, Entry]()

    val v1 = acquire(map, "t1", "actor-1")
    assertEquals(v1, "actor-1")
    assertEquals(map.get("t1").refCount.get(), 1)

    val v2 = acquire(map, "t1", "actor-2")
    assertEquals(v2, "actor-1")
    assertEquals(map.get("t1").refCount.get(), 2)

    assert(!release(map, "t1"))
    assertEquals(map.get("t1").refCount.get(), 1)

    assert(release(map, "t1"))
    assert(map.get("t1") == null)
  }

  test("release on missing key is no-op") {
    val map = new ConcurrentHashMap[String, Entry]()
    assert(!release(map, "missing"))
  }

  test("acquire after full release creates fresh entry") {
    val map = new ConcurrentHashMap[String, Entry]()

    acquire(map, "t1", "actor-1")
    release(map, "t1")
    assert(map.get("t1") == null)

    val v = acquire(map, "t1", "actor-2")
    assertEquals(v, "actor-2")
    assertEquals(map.get("t1").refCount.get(), 1)
  }

  test("concurrent acquire+release maintains correct refcount") {
    val map        = new ConcurrentHashMap[String, Entry]()
    val threads    = 100
    val barrier    = new CyclicBarrier(threads)
    val latch      = new CountDownLatch(threads)
    val errors     = new AtomicInteger(0)
    val acquireRef = new AtomicReference[String](null)

    // Pre-populate so all threads hit fast path
    acquire(map, "t1", "seed")
    acquireRef.set("seed")

    // Each thread: acquire, yield, release
    (0 until threads).foreach { i =>
      new Thread(() => {
        try {
          barrier.await()
          val v = acquire(map, "t1", s"actor-$i")
          if (v != "seed") errors.incrementAndGet()
          Thread.`yield`()
          release(map, "t1")
        } catch {
          case _: Exception => errors.incrementAndGet()
        } finally {
          latch.countDown()
        }
      }).start()
    }

    latch.await()
    assertEquals(errors.get(), 0)
    // Only seed refcount remains (1)
    assertEquals(map.get("t1").refCount.get(), 1)

    // Final release removes
    assert(release(map, "t1"))
    assert(map.get("t1") == null)
  }

  test("concurrent release never goes below zero") {
    val map     = new ConcurrentHashMap[String, Entry]()
    val threads = 50
    val barrier = new CyclicBarrier(threads)
    val latch   = new CountDownLatch(threads)
    val removed = new AtomicInteger(0)

    // Acquire 50 times
    acquire(map, "t1", "actor")
    (1 until threads).foreach(_ => acquire(map, "t1", "ignored"))
    assertEquals(map.get("t1").refCount.get(), threads)

    // Release all concurrently
    (0 until threads).foreach { _ =>
      new Thread(() => {
        try {
          barrier.await()
          if (release(map, "t1")) removed.incrementAndGet()
        } catch {
          case _: Exception => ()
        } finally {
          latch.countDown()
        }
      }).start()
    }

    latch.await()
    assertEquals(removed.get(), 1)
    assert(map.get("t1") == null)
  }

  // ==================== Two-level tests (matcher isolation) ====================

  test("two-level: different matchers on same topic get independent trackers") {
    val map      = new OuterMap()
    val matcher1 = new Object
    val matcher2 = new Object
    val ref1     = new IdentityKey(matcher1)
    val ref2     = new IdentityKey(matcher2)

    val v1 = acquire2(map, "reply-topic", ref1, "tracker-key")
    val v2 = acquire2(map, "reply-topic", ref2, "tracker-value")

    assertEquals(v1, "tracker-key")
    assertEquals(v2, "tracker-value")
    assertEquals(map.get("reply-topic").size(), 2)

    // Release one matcher — topic stays (other matcher still active)
    assert(!release2(map, "reply-topic", ref1))
    assert(map.get("reply-topic") != null)
    assertEquals(map.get("reply-topic").size(), 1)

    // Release the other — topic removed
    assert(release2(map, "reply-topic", ref2))
    assert(map.get("reply-topic") == null)
  }

  test("two-level: same matcher instance reuses tracker via refcount") {
    val map     = new OuterMap()
    val matcher = new Object
    val ref     = new IdentityKey(matcher)

    val v1 = acquire2(map, "topic", ref, "actor-1")
    val v2 = acquire2(map, "topic", ref, "actor-2")
    assertEquals(v1, "actor-1")
    assertEquals(v2, "actor-1")
    assertEquals(map.get("topic").get(ref).refCount.get(), 2)

    assert(!release2(map, "topic", ref))
    assertEquals(map.get("topic").get(ref).refCount.get(), 1)

    assert(release2(map, "topic", ref))
    assert(map.get("topic") == null)
  }

  test("two-level: IdentityKey distinguishes objects with same identityHashCode") {
    // Force collision by wrapping different objects with overridden hashCode
    class CollidingKey(val id: AnyRef) {
      override def hashCode(): Int           = 42
      override def equals(obj: Any): Boolean = obj.isInstanceOf[CollidingKey] && (id eq obj.asInstanceOf[CollidingKey].id)
    }

    val innerMap = new ConcurrentHashMap[CollidingKey, Entry]()
    val obj1     = new Object
    val obj2     = new Object
    val k1       = new CollidingKey(obj1)
    val k2       = new CollidingKey(obj2)

    innerMap.put(k1, Entry("a", new AtomicInteger(1)))
    innerMap.put(k2, Entry("b", new AtomicInteger(1)))

    assertEquals(innerMap.size(), 2)
    assertEquals(innerMap.get(k1).value, "a")
    assertEquals(innerMap.get(k2).value, "b")

    // Lookup with fresh wrapper for same ref works
    assertEquals(innerMap.get(new CollidingKey(obj1)).value, "a")

    // Lookup with wrapper for different ref does not cross-match
    assert(innerMap.get(new CollidingKey(new Object)) == null)
  }

  test("two-level: concurrent acquire+release across multiple matchers") {
    val map      = new OuterMap()
    val matchers = (0 until 4).map(_ => new Object)
    val refs     = matchers.map(new IdentityKey(_))
    val threads  = 100
    val barrier  = new CyclicBarrier(threads)
    val latch    = new CountDownLatch(threads)
    val errors   = new AtomicInteger(0)
    val topic    = "shared-reply"

    // Seed each matcher
    refs.zipWithIndex.foreach { case (ref, i) =>
      acquire2(map, topic, ref, s"seed-$i")
    }

    // Each thread picks a random matcher, acquires, yields, releases
    (0 until threads).foreach { i =>
      new Thread(() => {
        try {
          barrier.await()
          val ref  = refs(i % refs.length)
          val seed = s"seed-${i % refs.length}"
          val v    = acquire2(map, topic, ref, s"actor-$i")
          if (v != seed) errors.incrementAndGet()
          Thread.`yield`()
          release2(map, topic, ref)
        } catch {
          case _: Exception => errors.incrementAndGet()
        } finally {
          latch.countDown()
        }
      }).start()
    }

    latch.await()
    assertEquals(errors.get(), 0)

    // Each seed refcount remains at 1
    refs.foreach { ref =>
      assertEquals(map.get(topic).get(ref).refCount.get(), 1)
    }

    // Release all seeds — topic removed after last
    refs.init.foreach { ref =>
      assert(!release2(map, topic, ref))
    }
    assert(release2(map, topic, refs.last))
    assert(map.get(topic) == null)
  }

  test("two-level: release on wrong matcher is no-op") {
    val map      = new OuterMap()
    val matcher  = new Object
    val other    = new Object
    val ref      = new IdentityKey(matcher)
    val wrongRef = new IdentityKey(other)

    acquire2(map, "topic", ref, "actor")
    assert(!release2(map, "topic", wrongRef))
    // Original still present
    assertEquals(map.get("topic").get(ref).refCount.get(), 1)
  }
}
