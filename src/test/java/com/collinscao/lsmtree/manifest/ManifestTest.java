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

  /**
   * Verifies that SSTables are correctly registered in the in-memory metadata
   * and can be retrieved by their respective levels.
   */
  @Test
  void testAddAndGetSSTable() throws IOException {
    // 1. Setup: Flush a Memtable to create a physical SSTable file on disk.
    Memtable mem = new Memtable();
    mem.put("key1", "value1");
    SSTable sst = SSTable.createSSTableFromMemtable(mem, tempDir);

    // 2. Execution: Register the SSTable into Level 0.
    manifest.addSSTable(0, sst);

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

  /**
   * Validates the Manifest's core recovery logic.
   * Ensures that state is successfully persisted to the MANIFEST file
   * and can be restored after a simulated system restart.
   */
  @Test
  void testPersistenceAndRecovery() throws IOException {
    // --- Phase 1: Persistence ---

    // Create SSTable 1 (Level 0)
    Memtable mem1 = new Memtable();
    mem1.put("a", "1");
    SSTable sst1 = SSTable.createSSTableFromMemtable(mem1, tempDir);
    manifest.addSSTable(0, sst1);

    // Create SSTable 2 (Level 1)
    Memtable mem2 = new Memtable();
    mem2.put("b", "2");
    SSTable sst2 = SSTable.createSSTableFromMemtable(mem2, tempDir);
    manifest.addSSTable(1, sst2);

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
}