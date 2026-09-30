package com.acme.transfer.kit;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 * Helpers for testing code that runs on virtual threads, without sleeping: tests wait for
 * something to happen instead of waiting for time to pass.
 */
public final class VirtualThreads {

  private static final Duration WAIT = Duration.ofSeconds(5);

  private VirtualThreads() {
  }

  /** Waits for the latch to open. Fails the test instead of hanging when it never does. */
  public static void await(CountDownLatch latch) {
    try {
      if (!latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new AssertionError("Latch still at " + latch.getCount() + " after " + WAIT);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  /**
   * Starts {@code count} virtual threads that all begin at the same moment, and waits until every
   * one has finished.
   */
  public static void runTogether(int count, IntConsumer task) {
    CountDownLatch ready = new CountDownLatch(count);
    CountDownLatch go = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      int index = i;
      Thread.ofVirtual().name("test-" + i).start(() -> {
        ready.countDown();
        await(go);
        try {
          task.accept(index);
        } finally {
          done.countDown();
        }
      });
    }
    await(ready);
    go.countDown();
    await(done);
  }

  /** Counts how many callers are inside a section right now, and the most there have been. */
  public static final class ConcurrencyProbe {

    private final AtomicInteger inside = new AtomicInteger();
    private final AtomicInteger most = new AtomicInteger();

    public void enter() {
      most.accumulateAndGet(inside.incrementAndGet(), Math::max);
    }

    public void exit() {
      inside.decrementAndGet();
    }

    public int inside() {
      return inside.get();
    }

    public int most() {
      return most.get();
    }
  }
}
