package org.galaxio.gatling.kafka.client

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, CyclicBarrier}

/** Tests the ConcurrentHashMap refcount algorithm used by KafkaMessageTrackerPool. Exercises the same computeIfPresent/compute
  * pattern against concurrent acquire+release.
  */
class TrackerRefCountSpec extends munit.FunSuite {

  private case class Entry(value: String, refCount: AtomicInteger)

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
}
