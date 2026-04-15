package com.collinscao.lsmtree.sstable;

import com.collinscao.memtable.Memtable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.util.Constants;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for SSTable Bloom Filter functionality.
 */
class SSTableBloomFilterTest {

  @TempDir
  Path tempDir;

  private SSTable sstable;
  private final String KEY_1 = "apple";
  private final String KEY_2 = "cherry";
  private final String KEY_3 = "elderberry";

  // Key falls within the [KEY_1, KEY_3] range but is not present in the table.
  private final String ABSENT_KEY = "banana";

  @BeforeEach
  void setUp() throws IOException {
    Memtable mem = new Memtable();
    mem.put(KEY_1, "value1");
    mem.put(KEY_2, "value2");
    mem.put(KEY_3, "value3");

    // Flush Memtable to create SSTable and trigger the initial Bloom Filter construction.
    sstable = SSTable.createSSTableFromMemtable(mem, tempDir);
  }

  @Test
  void testBloomFilterPositive() {
    // Verify that existing keys are accessible (no false negatives from Bloom Filter).
    assertEquals("value1", sstable.get(KEY_1));
    assertEquals("value2", sstable.get(KEY_2));
    assertEquals("value3", sstable.get(KEY_3));
  }

  @Test
  void testBloomFilterNegative() {
    /**
     * Verify Bloom Filter filtering capability.
     * "banana" is between "apple" and "elderberry", thus bypassing the min/max range check.
     * The Bloom Filter must correctly identify this key as absent.
     */
    assertNull(sstable.get(ABSENT_KEY), "Bloom Filter should intercept and return null for absent key.");
  }

  @Test
  void testBloomFilterAfterRecovery() throws IOException {
    // 1. Capture file path and clear current instance.
    Path path = sstable.getFilePath();
    sstable = null;

    // 2. Simulate system restart: reload SSTable and trigger Bloom Filter reconstruction via init().
    SSTable recoveredSstable = new SSTable(path);

    // 3. Verify that the reconstructed filter remains functional.
    // Check for existing keys.
    assertEquals("value2", recoveredSstable.get(KEY_2));
    // Check for absent key within the key range.
    assertNull(recoveredSstable.get(ABSENT_KEY), "Reconstructed Bloom Filter should still intercept absent keys.");
  }

  @Test
  void testRangeCheckStillWorks() {
    // Verify primary range guard (min/max check) which occurs before Bloom Filter evaluation.
    assertNull(sstable.get("aaaaa"), "Should return null for keys smaller than minKey.");
    assertNull(sstable.get("zzzzz"), "Should return null for keys larger than maxKey.");
  }
}