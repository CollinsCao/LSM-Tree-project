package com.collinscao.lsmtree.sstable;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.memtable.Memtable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Integration tests for {@link SSTableService}, focusing on multi-level
 * lookup logic and version precedence within the LSM tree.
 */
class SSTableServiceTest {

  @TempDir
  Path tempDir;

  private SSTableService ssTableService;
  private Manifest manifest;

  @BeforeEach
  void setUp() throws IOException {
    // 1. Initialize the Manifest to track table metadata
    manifest = new Manifest(tempDir.toString());

    // 2. Prepare mock data: Simulate flushing two Memtables into SSTables

    // Older SSTable (sstOld): Contains a key "a" that will eventually be shadowed
    Memtable memOld = new Memtable();
    memOld.put("a", "old_val"); // Shadowed key
    memOld.put("c", "val_c");   // Unique key in older tier
    SSTable sstOld = SSTable.createSSTableFromMemtable(memOld, tempDir);
    manifest.applyFlush(0, sstOld);

    // Newer SSTable (sstNew): Appended later to Level 0.
    // In Level 0, newer files are added to the end of the list;
    // lookup must iterate in reverse order (from tail to head) for correct versioning.
    Memtable memNew = new Memtable();
    memNew.put("a", "new_val"); // Overwrites/shadows older value
    memNew.put("b", "val_b");   // Unique key in newer tier
    SSTable sstNew = SSTable.createSSTableFromMemtable(memNew, tempDir);
    manifest.applyFlush(0, sstNew);

    // 3. Initialize Service with the populated manifest
    ssTableService = new SSTableService(manifest);
  }

  @AfterEach
  void tearDown() {
    // Shut down background thread pool to prevent thread leaks across test cases
    if (ssTableService != null) {
      ssTableService.stop();
    }
  }

  @Test
  void testGetOverwrite() {
    // Verify L0 shadowing: The most recent version (from sstNew) must take precedence
    String val = ssTableService.get("a");
    assertEquals("new_val", val, "Should retrieve the most recent value (shadowing check)");
  }

  @Test
  void testGetFallThrough() {
    // Verify fall-through: If a key is missing in newer SSTables,
    // the search should successfully continue into older files.
    String val = ssTableService.get("c");
    assertEquals("val_c", val, "Should fall through to the older SSTable to find the value");
  }

  @Test
  void testGetNewKey() {
    // Verify lookup for a key existing only in the most recent SSTable
    String val = ssTableService.get("b");
    assertEquals("val_b", val, "Should retrieve value from the most recent SSTable");
  }

  @Test
  void testGetNonExistent() {
    // Verify behavior when a key is absent across all SSTable tiers
    String val = ssTableService.get("z");
    assertNull(val, "Lookup for a non-existent key should return null");
  }
}