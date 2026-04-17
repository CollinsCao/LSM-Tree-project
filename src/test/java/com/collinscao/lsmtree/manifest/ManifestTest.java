package com.collinscao.lsmtree.manifest;

import static org.junit.jupiter.api.Assertions.*;
import com.collinscao.lsmtree.memtable.Memtable;
import com.collinscao.lsmtree.sstable.SSTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;

/**
 * Unit tests for {@link Manifest} to ensure metadata persistence
 * and LSM-tree structure integrity.
 */
class ManifestTest {

  private Manifest manifest;

  /**
   * Managed temporary directory for test artifacts.
   * Automatically cleaned up by JUnit 5 after each test execution.
   */
  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    // Initialize a fresh Manifest instance pointing to the isolated temp directory.
    manifest = new Manifest(tempDir.toString());
  }

  @AfterEach
  void tearDown() {
    // Explicitly nullify the reference to aid GC and ensure isolation.
    manifest = null;
  }

  /** Tests adding and retrieving SSTables in specific levels. */
  /**
   * Verifies that SSTables are correctly registered in the in-memory metadata
   * and can be retrieved by their respective levels.
   */
  @Test
  @DisplayName("Add and retrieve SSTables by level")
  void testAddAndGetSSTable() throws IOException {
    // 1. Setup: Flush a Memtable to create a physical SSTable file on disk.
    Memtable mem = new Memtable();
    mem.put("key1", "value1");
    SSTable sst = SSTable.createSSTableFromMemtable(mem, tempDir);

    // 2. Execution: Register the SSTable into Level 0.
    manifest.applyFlush(0, sst);

    // 3. Verification: Ensure metadata reflects the addition accurately.
    List<SSTable> level0 = manifest.getSSTable(0);
    assertEquals(1, level0.size(), "Level 0 should contain exactly one SSTable.");

    // Assert that the file path in metadata matches the physical file created.
    String expectedName = sst.getFilePath().getFileName().toString();
    String actualName = level0.get(0).getFilePath().getFileName().toString();
    assertEquals(expectedName, actualName, "SSTable filename in Manifest should match source.");

    // Assert no leakage to adjacent levels.
    List<SSTable> level1 = manifest.getSSTable(1);
    assertTrue(level1.isEmpty(), "Level 1 should remain empty.");
  }

  /** Tests persistence and recovery after simulated restart. */
  /**
   * Validates the Manifest's core recovery logic.
   * Ensures that state is successfully persisted to the MANIFEST file
   * and can be restored after a simulated system restart.
   */
  @Test
  @DisplayName("Persist and recover Manifest state")
  void testPersistenceAndRecovery() throws IOException {
    // --- Phase 1: Persistence ---

    // Create SSTable 1 (Level 0)
    Memtable mem1 = new Memtable();
    mem1.put("a", "1");
    SSTable sst1 = SSTable.createSSTableFromMemtable(mem1, tempDir);
    manifest.applyFlush(0, sst1);

    // Create SSTable 2 (Level 1)
    Memtable mem2 = new Memtable();
    mem2.put("b", "2");
    SSTable sst2 = SSTable.createSSTableFromMemtable(mem2, tempDir);
    manifest.applyFlush(1, sst2);

    // Simulate system shutdown by discarding the current Manifest instance.
    manifest = null;

    // --- Phase 2: Recovery ---

    // Re-instantiate Manifest; this triggers internal recovery from CURRENT/MANIFEST files.
    Manifest recoveredManifest = new Manifest(tempDir.toString());

    // --- Phase 3: Verification ---

    // Verify Level 0 metadata and data accessibility.
    List<SSTable> l0 = recoveredManifest.getSSTable(0);
    assertEquals(1, l0.size(), "Level 0 should recover 1 file.");
    assertEquals("1", l0.get(0).get("a"), "Data should be reachable via recovered SSTable metadata.");

    // Verify Level 1 metadata and data accessibility.
    List<SSTable> l1 = recoveredManifest.getSSTable(1);
    assertEquals(1, l1.size(), "Level 1 should recover 1 file.");
    assertEquals("2", l1.get(0).get("b"), "Data should be reachable via recovered SSTable metadata.");
  }

  /** Tests concurrent read/write operations on Manifest metadata. */
  @Test
  @DisplayName("Verify metadata consistency under concurrent read/write operations")
  void testConcurrentReadWrite() throws InterruptedException {
    int writeThreads = 5;
    int readThreads = 5;
    int operationsPerThread = 100;

    // Use a thread pool to simulate concurrent execution
    ExecutorService executor = Executors.newFixedThreadPool(writeThreads + readThreads);
    // Use CountDownLatches to coordinate a simultaneous start and wait for completion
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(writeThreads + readThreads);

    AtomicInteger errorCount = new AtomicInteger(0);

    // 1. Define Writer Threads: Continuously invoke applyFlush
    for (int i = 0; i < writeThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await(); // Wait for the synchronized start signal
          for (int j = 0; j < operationsPerThread; j++) {
            // Simulate creating an SSTable from a Memtable
            Memtable m = new Memtable();
            m.put("key-" + threadId + "-" + j, "val");
            SSTable sst = SSTable.createSSTableFromMemtable(m, tempDir);

            // Persist the SSTable metadata to L0 in the Manifest
            manifest.applyFlush(0, sst);
          }
        } catch (Exception e) {
          e.printStackTrace();
          errorCount.incrementAndGet();
        } finally {
          endLatch.countDown();
        }
      });
    }

    // 2. Define Reader Threads: Continuously invoke getLevelSnapshot and iterate
    for (int i = 0; i < readThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < operationsPerThread * 2; j++) {
            // Retrieve a snapshot of all levels
            Map<Integer, List<SSTable>> snapshot = manifest.getLevelSnapshot();

            /*
             * Core Validation: Iterate through the snapshot.
             * If the Manifest implementation lacks defensive copying,
             * a ConcurrentModificationException will be triggered here.
             */
            for (List<SSTable> tables : snapshot.values()) {
              for (SSTable sst : tables) {
                assertNotNull(sst.getFilePath());
              }
            }
            // Brief pause to increase the probability of thread interleaving
            Thread.sleep(1);
          }
        } catch (Exception e) {
          e.printStackTrace();
          errorCount.incrementAndGet();
        } finally {
          endLatch.countDown();
        }
      });
    }

    // Signal all threads to start processing
    startLatch.countDown();

    // Wait for all threads to complete, with a 30-second timeout safety net
    boolean finished = endLatch.await(30, TimeUnit.SECONDS);

    assertTrue(finished, "Test timed out; a deadlock might have occurred.");
    assertEquals(0, errorCount.get(), "Exceptions occurred during concurrent execution.");

    // Final verification: Ensure the total number of L0 files matches expected count
    List<SSTable> l0 = manifest.getSSTable(0);
    assertEquals(writeThreads * operationsPerThread, l0.size(), "Final L0 file count mismatch.");

    executor.shutdown();
  }
}