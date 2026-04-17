package com.collinscao.lsmtree.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * High-intensity stress tests to validate LSM-Tree stability during
 * concurrent memtable flushes and background SSTable compactions.
 */
class DBConcurrencyTest {

  @TempDir
  Path tempDir;

  private DB db;

  @BeforeEach
  void setUp() throws IOException {
    db = new DB(tempDir.toString());
  }

  @AfterEach
  void tearDown() throws IOException {
    if (db != null) {
      db.close();
    }
  }

  @Test
  @DisplayName("High concurrency read/write triggering background flush and compaction")
  void testConcurrentReadWriteWithCompaction() throws InterruptedException {
    int writeThreads = 5;
    int readThreads = 5;
    int keysPerThread = 200;

    // Construct a large payload relative to the memtable size threshold (Constants.MAXSIZE_MEMTABLE).
    // With 200-byte values, every ~5 puts will trigger a flush, generating high volume
    // of L0 SSTables to stress-test the background compaction logic.
    String largeValue = "V".repeat(200);

    ExecutorService executor = Executors.newFixedThreadPool(writeThreads + readThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(writeThreads + readThreads);
    AtomicInteger errorCount = new AtomicInteger(0);

    // 1. Initialize writer threads
    for (int i = 0; i < writeThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < keysPerThread; j++) {
            db.put("stress_key_" + threadId + "_" + j, largeValue + "_" + j);
          }
        } catch (Exception e) {
          e.printStackTrace();
          errorCount.incrementAndGet();
        } finally {
          endLatch.countDown();
        }
      });
    }

    // 2. Initialize reader threads to perform lookups during active file rotation and merging
    for (int i = 0; i < readThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < keysPerThread; j++) {
            String key = "stress_key_" + threadId + "_" + j;
            String val = db.get(key);

            // Note: Since R/W is fully concurrent, values may be null if the key is not yet written.
            // If data is returned, it must be consistent and corruption-free.
            if (val != null) {
              if (!val.equals(largeValue + "_" + j)) {
                System.err.println("Data corruption detected for key: " + key);
                errorCount.incrementAndGet();
              }
            }
            Thread.sleep(1); // Increase intersection probability with background compaction threads
          }
        } catch (Exception e) {
          e.printStackTrace();
          errorCount.incrementAndGet();
        } finally {
          endLatch.countDown();
        }
      });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Await completion with a 30-second safety timeout to detect potential deadlocks
    boolean finished = endLatch.await(30, TimeUnit.SECONDS);

    assertTrue(finished, "Test timed out. Possible deadlock in DB engine.");
    assertEquals(0, errorCount.get(), "Exceptions or data corruption occurred during concurrent stress test.");

    // 3. Final consistency check: allow background tasks to settle before full scan
    Thread.sleep(2000);

    for (int i = 0; i < writeThreads; i++) {
      for (int j = 0; j < keysPerThread; j++) {
        String expectedVal = largeValue + "_" + j;
        String actualVal = db.get("stress_key_" + i + "_" + j);
        assertEquals(expectedVal, actualVal, "Final data consistency check failed.");
      }
    }
  }
}