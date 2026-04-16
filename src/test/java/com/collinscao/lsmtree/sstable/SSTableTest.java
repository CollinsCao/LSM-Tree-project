package com.collinscao.lsmtree.sstable;

import static org.junit.jupiter.api.Assertions.*;


import com.collinscao.lsmtree.memtable.Memtable;
import com.util.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.io.IOException;
import java.nio.file.Path;

class SSTableTest {

  private SSTable sstable;
  private static final String LARGE_VAL = "large_value_".repeat(50);

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
    memtable.put("fig", LARGE_VAL);

    // Create SSTable with tempDir
    sstable = SSTable.createSSTableFromMemtable(memtable, tempDir);
  }

  @AfterEach
  void tearDown() {
    // @TempDir，JUnit will clean up generated SSTable files automatically.
  }

  @Test
  void testSSTablePathGeneration(@TempDir Path tempDir) {
    Path p1 = SSTable.generateSSTablePath(tempDir);
    Path p2 = SSTable.generateSSTablePath(tempDir);

    assertTrue(p1.getFileName().toString().startsWith(Constants.SSTABLE_PREFIX));
    assertTrue(p1.toString().endsWith(Constants.SSTABLE_FILE_EXTENSION));
    assertNotEquals(p1, p2, "Filenames should be unique.");
    assertEquals(tempDir, p1.getParent(), "The path should be in the temporary directory.");
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
    String actual = sstable.get("fig");
    assertNotNull(actual);
    assertEquals(LARGE_VAL, actual, "Should correctly retrieve large string values");
  }

  @Test
  void testFileExists() {
    Path path = sstable.getFilePath();
    assertTrue(Files.exists(path), "SSTable file should be created on disk");
    try {
      assertTrue(Files.size(path) > 0, "SSTable file should not be empty");
    } catch (IOException e) {
      throw new RuntimeException("Failed to get file's size", e);
    }
  }

  @Test
  void testInitRecovery() throws IOException {
    // Arrange: Verify existing SSTable file from setUp
    Path path = sstable.getFilePath();
    assertTrue(Files.exists(path), "SSTable file should exist before recovery");

    // Act: Simulate system restart by dropping reference and reloading from disk
    sstable = null;
    SSTable recoveredSSTable = new SSTable(path);

    // Assert: Verify metadata and data integrity after reconstruction
    assertNotNull(recoveredSSTable.getFilePath());

    // Verify point lookups for existing keys
    assertEquals("red_fruit", recoveredSSTable.get("apple"), "Recovery failed for key: apple");
    assertEquals("yellow_fruit", recoveredSSTable.get("banana"), "Recovery failed for key: banana");
    assertEquals(LARGE_VAL, recoveredSSTable.get("fig"), "Recovery failed for key: fig");

    // Verify non-existent keys
    assertNull(recoveredSSTable.get("zebra"), "Lookup should return null for non-existent key after recovery");
  }
}