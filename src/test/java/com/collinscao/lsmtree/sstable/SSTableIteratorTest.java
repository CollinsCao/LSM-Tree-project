package com.collinscao.lsmtree.sstable;

import com.collinscao.memtable.Memtable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SSTableIterator} to ensure correct data retrieval
 * from persisted SSTable files.
 */
class SSTableIteratorTest {

  @TempDir
  Path tempDir;

  private SSTable sstable;
  private TreeMap<String, String> expectedData;

  @BeforeEach
  void setUp() throws IOException {
    // Initialize sorted test dataset
    expectedData = new TreeMap<>();
    expectedData.put("apple", "red");
    expectedData.put("banana", "yellow");
    expectedData.put("cherry", "red_dark");
    expectedData.put("date", "brown");

    // Flush Memtable to create an SSTable for testing
    Memtable mem = new Memtable();
    expectedData.forEach(mem::put);
    sstable = SSTable.createSSTableFromMemtable(mem, tempDir);
  }

  @Test
  void testIterationAllEntries() throws IOException {
    // Verify that the iterator traverses all entries in the correct order
    try (SSTableIterator iterator = new SSTableIterator(sstable)) {
      int count = 0;
      for (Map.Entry<String, String> entry : expectedData.entrySet()) {
        assertTrue(iterator.hasNext(), "Iterator should have more elements");
        Map.Entry<String, String> actualEntry = iterator.next();

        assertEquals(entry.getKey(), actualEntry.getKey(), "Key mismatch during iteration");
        assertEquals(entry.getValue(), actualEntry.getValue(), "Value mismatch for key: " + entry.getKey());
        count++;
      }

      // Verify end-of-stream state
      assertFalse(iterator.hasNext(), "Iterator should be exhausted");
      assertEquals(expectedData.size(), count, "Total iterated count should match source data size");
    }
  }

  @Test
  void testNoSuchElementException() throws IOException {
    try (SSTableIterator iterator = new SSTableIterator(sstable)) {
      // Consume all elements
      while (iterator.hasNext()) {
        iterator.next();
      }

      // Ensure next() throws exception when no more elements are available
      assertThrows(NoSuchElementException.class, iterator::next,
          "Calling next() on an exhausted iterator should throw NoSuchElementException");
    }
  }

  @Test
  void testEmptySSTable() throws IOException {
    // Test iterator behavior with an empty SSTable
    Memtable emptyMem = new Memtable();
    SSTable emptySst = SSTable.createSSTableFromMemtable(emptyMem, tempDir);

    try (SSTableIterator iterator = new SSTableIterator(emptySst)) {
      assertFalse(iterator.hasNext(), "hasNext() should return false for an empty SSTable");
    }
  }

  @Test
  void testLargeValueIteration() throws IOException {
    // Validate that large values (potentially exceeding block sizes) are handled correctly
    Memtable mem = new Memtable();
    String largeKey = "key_large";
    String largeValue = "v".repeat(5000);
    mem.put(largeKey, largeValue);

    SSTable sst = SSTable.createSSTableFromMemtable(mem, tempDir);

    try (SSTableIterator iterator = new SSTableIterator(sst)) {
      assertTrue(iterator.hasNext());
      Map.Entry<String, String> entry = iterator.next();
      assertEquals(largeKey, entry.getKey());
      assertEquals(largeValue, entry.getValue());
    }
  }
}