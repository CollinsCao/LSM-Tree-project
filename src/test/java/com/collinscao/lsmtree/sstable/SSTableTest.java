package com.collinscao.lsmtree.sstable;

import static org.junit.jupiter.api.Assertions.*;


import com.collinscao.memtable.Memtable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

class SSTableTest {

  private SSTable sstable;

  /**
   * Automatic temporary directory management via JUnit 5.
   */
  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    // Prepare Memtable with sample data
    Memtable memtable = new Memtable();
    memtable.put("apple", "red_fruit");
    memtable.put("banana", "yellow_fruit");
    memtable.put("cherry", "red_small_fruit");
    memtable.put("date", "sweet_fruit");
    memtable.put("elderberry", "purple_fruit");

    // Populate large value for cross-block read/write verification
    memtable.put("fig", "large_value_".repeat(50));

    // Create SSTable with tempDir
    sstable = SSTable.createSSTableFromMemtable(memtable, tempDir);
  }

  @AfterEach
  void tearDown() {
    // @TempDir，JUnit will clean up generated SSTable files automatically.
  }

  @Test
  void testGetExistingKeys() {
    assertEquals("red_fruit", sstable.get("apple"), "Should return the correct value for 'apple'");
    assertEquals("yellow_fruit", sstable.get("banana"), "Should return the correct value for 'banana'");
    assertEquals("purple_fruit", sstable.get("elderberry"), "Should return the correct value for 'elderberry'");
  }

  @Test
  void testGetNonExistingKeys() {
    assertNull(sstable.get("avocado"), "Should return null for non-existent key (lower bound)");
    assertNull(sstable.get("cat"), "Should return null for non-existent key (middle range)");
    assertNull(sstable.get("zebra"), "Should return null for non-existent key (upper bound)");
  }

  @Test
  void testGetLargeValue() {
    String expected = new StringBuilder().repeat("large_value_", 50).toString();

    String actual = sstable.get("fig");
    assertNotNull(actual);
    assertEquals(expected, actual, "Should correctly retrieve large string values");
  }

  @Test
  void testFileExists() {
    File f = new File(sstable.getFilePath());
    assertTrue(f.exists(), "SSTable file should be created on disk");
    assertTrue(f.length() > 0, "SSTable file should not be empty");
  }
}