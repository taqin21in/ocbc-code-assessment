package com.acme.transfer.kit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Worked examples for {@link VirtualThreads}, on a toy printer room with two printers. */
class VirtualThreadExamplesTest {

  @Test
  void codeRunsOnAVirtualThread() throws InterruptedException {
    AtomicBoolean virtual = new AtomicBoolean();

    Thread thread = Thread.ofVirtual().start(() -> virtual.set(Thread.currentThread().isVirtual()));
    thread.join();

    assertTrue(virtual.get());
  }

  @Test
  void aSemaphoreLimitsHowManyJobsRunAtOnce() {
    Semaphore printers = new Semaphore(2, true);
    VirtualThreads.ConcurrencyProbe printing = new VirtualThreads.ConcurrencyProbe();
    CountDownLatch bothPrintersBusy = new CountDownLatch(2);
    CountDownLatch paper = new CountDownLatch(1);
    AtomicInteger printingWhenFull = new AtomicInteger();

    // Hands out paper only once both printers are busy, so the test sees the limit reached.
    Thread.ofVirtual().start(() -> {
      VirtualThreads.await(bothPrintersBusy);
      printingWhenFull.set(printing.inside());
      paper.countDown();
    });

    VirtualThreads.runTogether(10, job -> {
      acquire(printers);
      try {
        printing.enter();
        bothPrintersBusy.countDown();
        VirtualThreads.await(paper);
        printing.exit();
      } finally {
        printers.release();
      }
    });

    assertEquals(2, printingWhenFull.get());
    assertEquals(2, printing.most());
  }

  private static void acquire(Semaphore semaphore) {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
