package com.collinscao.lsmtree.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying the Tombstone (logical deletion) mechanism.
 */
class TombstoneIntegrationTest {

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
  @DisplayName("Verify that tombstones successfully mask older data across Memtable and SSTables")
  void testTombstoneMasking() throws IOException {
    int keyCount = 100;

    // 1. Insert initial data set into the active memtable
    for (int i = 0; i < keyCount; i++) {
      db.put("key_" + i, "value_" + i);
    }

    // Verify data is retrievable while still in the volatile memtable
    assertEquals("value_50", db.get("key_50"));

    // 2. Simulate a restart to force a flush of the memtable to a persistent SSTable
    db.close();
    db = new DB(tempDir.toString());

    // Ensure data remains accessible after recovery from persistent storage
    assertEquals("value_50", db.get("key_50"));

    // 3. Execute deletions by writing tombstones to the new active memtable
    for (int i = 0; i < keyCount; i++) {
      db.remove("key_" + i);
    }

    // Verify that tombstones successfully mask underlying values in the volatile layer
    assertNull(db.get("key_50"), "Data should be null immediately after removal (masked by memtable tombstone)");

    // 4. Simulate another restart to flush tombstones to a newer SSTable tier
    db.close();
    db = new DB(tempDir.toString());

    // Verify multi-level shadowing: tombstones in newer SSTables must mask data in older SSTables
    for (int i = 0; i < keyCount; i++) {
      assertNull(db.get("key_" + i), "Data should remain null after tombstones are flushed to disk");
    }
  }
}