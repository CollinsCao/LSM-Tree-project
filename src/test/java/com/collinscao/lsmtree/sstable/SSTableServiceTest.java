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

    // 2. Prepare mock data: Simulate the flushing of two Memtables into SSTables

    // Older SSTable (sstOld): Contains a key "a" that will eventually be shadowed
    Memtable memOld = new Memtable();
    memOld.put("a", "old_val"); // Shadowed key
    memOld.put("c", "val_c");   // Unique key in older tier
    SSTable sstOld = SSTable.createSSTableFromMemtable(memOld, tempDir);
    manifest.applyFlush(0, sstOld);

    // Newer SSTable (sstNew): Flushed later.
    // In Level 0, newer files are prepended to ensure higher priority during lookups.
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
    // Resources managed by @TempDir are automatically cleaned up
  }

  @Test
  void testGetOverwrite() {
    // Verify shadowing logic in L0: The most recent version must be returned.
    // Since sstNew is at the head of the list, it should be hit first.
    String val = ssTableService.get("a");
    assertEquals("new_val", val, "Should retrieve the most recent value (shadowing check)");
  }

  @Test
  void testGetFallThrough() {
    // Verify fall-through lookup: If a key is missing in newer SSTables,
    // the search should continue into older files.
    String val = ssTableService.get("c");
    assertEquals("val_c", val, "Should fall through to the older SSTable to find the value");
  }

  @Test
  void testGetNewKey() {
    // Verify lookup for a key existing only in the most recent SSTable.
    String val = ssTableService.get("b");
    assertEquals("val_b", val, "Should retrieve value from the most recent SSTable");
  }

  @Test
  void testGetNonExistent() {
    // Verify behavior when a key is absent across all SSTable tiers.
    String val = ssTableService.get("z");
    assertNull(val, "Lookup for a non-existent key should return null");
  }
}