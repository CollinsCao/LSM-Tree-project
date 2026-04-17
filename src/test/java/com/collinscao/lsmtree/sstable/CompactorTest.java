package com.collinscao.lsmtree.sstable;


import com.collinscao.lsmtree.memtable.Memtable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static org.junit.jupiter.api.Assertions.*;


public class CompactorTest {

  @TempDir // JUnit 5 provides a temporary directory for tests
  Path tempDir;

  private Compactor compactor;

  @BeforeEach
  void setUp() {
    compactor = new Compactor();
  }

  // Helper method to create a Memtable with given data
  private Memtable createMemtable(Map<String, String> data) {
    Memtable memtable = new Memtable();
    data.forEach(memtable::put);
    return memtable;
  }

  // Helper method to create an SSTable from a Memtable
  private SSTable createSSTableFromMap(Map<String, String> data, Path directory)
      throws IOException {
    Memtable memtable = createMemtable(data);
    return SSTable.createSSTableFromMemtable(memtable, directory);
  }

  // Helper method to read an SSTable's content into a Map for easy comparison
  private Map<String, String> readSSTableToMap(SSTable sstable) throws IOException {
    Map<String, String> map = new TreeMap<>();
    try (SSTableIterator iterator = new SSTableIterator(sstable)) {
      while (iterator.hasNext()) {
        Map.Entry<String, String> entry = iterator.next();
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  /** Tests compaction of SSTables with no overlapping keys. */
  @Test
  @DisplayName("Compact two SSTables with no overlapping keys")
  void testCompactNoOverlap() throws IOException {
    // SSTable 1 (older)
    Map<String, String> data1 = new TreeMap<>();
    data1.put("apple", "red");
    data1.put("banana", "yellow");
    SSTable sstable1 = createSSTableFromMap(data1, tempDir);

    // SSTable 2 (newer)
    Map<String, String> data2 = new TreeMap<>();
    data2.put("grape", "purple");
    data2.put("orange", "orange");
    SSTable sstable2 = createSSTableFromMap(data2, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1,
        sstable2); // Order matters: sstable1 is older

    Path outputSSTablePath = tempDir.resolve("compacted_no_overlap.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    // Verify content
    Map<String, String> expected = new TreeMap<>();
    expected.put("apple", "red");
    expected.put("banana", "yellow");
    expected.put("grape", "purple");
    expected.put("orange", "orange");

    assertEquals(expected, readSSTableToMap(compactedSSTable));
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction with overlapping keys where newer values win. */
  @Test
  @DisplayName("Compact two SSTables with overlapping keys, newer should win")
  void testCompactWithOverlapNewerWins() throws IOException {
    // SSTable 1 (older)
    Map<String, String> data1 = new TreeMap<>();
    data1.put("key1", "value1_old");
    data1.put("key2", "value2_old");
    data1.put("key3", "value3_old");
    SSTable sstable1 = createSSTableFromMap(data1, tempDir);

    // SSTable 2 (newer)
    Map<String, String> data2 = new TreeMap<>();
    data2.put("key1", "value1_new"); // Overlaps with sstable1, newer should win
    data2.put("key3", "value3_newer"); // Overlaps with sstable1, newer should win
    data2.put("key4", "value4_new"); // New key
    SSTable sstable2 = createSSTableFromMap(data2, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1, sstable2); // sstable1 is older

    Path outputSSTablePath = tempDir.resolve("compacted_overlap.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    // Verify content: key1 and key3 should have values from sstable2
    Map<String, String> expected = new TreeMap<>();
    expected.put("key1", "value1_new");
    expected.put("key2", "value2_old");
    expected.put("key3", "value3_newer");
    expected.put("key4", "value4_new");

    assertEquals(expected, readSSTableToMap(compactedSSTable));
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction of multiple SSTables with complex overlaps. */
  @Test
  @DisplayName("Compact multiple SSTables with complex overlaps")
  void testCompactMultipleSSTables() throws IOException {
    // SSTable 1 (oldest)
    Map<String, String> data1 = new TreeMap<>();
    data1.put("a", "1");
    data1.put("b", "2");
    data1.put("c", "3");
    SSTable sstable1 = createSSTableFromMap(data1, tempDir);

    // SSTable 2 (middle)
    Map<String, String> data2 = new TreeMap<>();
    data2.put("b", "2.1"); // Overrides b from sstable1
    data2.put("d", "4");
    data2.put("e", "5");
    SSTable sstable2 = createSSTableFromMap(data2, tempDir);

    // SSTable 3 (newest)
    Map<String, String> data3 = new TreeMap<>();
    data3.put("a", "1.1"); // Overrides a from sstable1
    data3.put("c", "3.1"); // Overrides c from sstable1
    data3.put("d", "4.1"); // Overrides d from sstable2
    data3.put("f", "6");
    SSTable sstable3 = createSSTableFromMap(data3, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1, sstable2, sstable3); // Order matters

    Path outputSSTablePath = tempDir.resolve("compacted_multi.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    // Expected results: newest values should win
    Map<String, String> expected = new TreeMap<>();
    expected.put("a", "1.1"); // From sstable3
    expected.put("b", "2.1"); // From sstable2
    expected.put("c", "3.1"); // From sstable3
    expected.put("d", "4.1"); // From sstable3
    expected.put("e", "5");   // From sstable2
    expected.put("f", "6");   // From sstable3

    assertEquals(expected, readSSTableToMap(compactedSSTable));
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction of a single SSTable. */
  @Test
  @DisplayName("Compact with a single SSTable")
  void testCompactSingleSSTable() throws IOException {
    Map<String, String> data = new TreeMap<>();
    data.put("single1", "value1");
    data.put("single2", "value2");
    SSTable sstable = createSSTableFromMap(data, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable);

    Path outputSSTablePath = tempDir.resolve("compacted_single.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    assertEquals(data, readSSTableToMap(compactedSSTable));
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction of empty SSTables. */
  @Test
  @DisplayName("Compact with empty SSTables")
  void testCompactEmptySSTables() throws IOException {
    SSTable sstable1 = createSSTableFromMap(new TreeMap<>(), tempDir);
    SSTable sstable2 = createSSTableFromMap(new TreeMap<>(), tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1, sstable2);

    Path outputSSTablePath = tempDir.resolve("compacted_empty.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    assertTrue(readSSTableToMap(compactedSSTable).isEmpty());
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction with mixed empty and non-empty SSTables. */
  @Test
  @DisplayName("Compact with some empty and some non-empty SSTables")
  void testCompactMixedEmptyNonEmpty() throws IOException {
    Map<String, String> data1 = new TreeMap<>();
    data1.put("a", "1");
    SSTable sstable1 = createSSTableFromMap(data1, tempDir);

    SSTable sstable2 = createSSTableFromMap(new TreeMap<>(), tempDir); // Empty

    Map<String, String> data3 = new TreeMap<>();
    data3.put("b", "2");
    data3.put("c", "3");
    SSTable sstable3 = createSSTableFromMap(data3, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1, sstable2, sstable3);

    Path outputSSTablePath = tempDir.resolve("compacted_mixed.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    Map<String, String> expected = new TreeMap<>();
    expected.put("a", "1");
    expected.put("b", "2");
    expected.put("c", "3");

    assertEquals(expected, readSSTableToMap(compactedSSTable));
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }

  /** Tests compaction removing keys marked as tombstones. */
  @Test
  @DisplayName("Compaction should remove keys marked as DELETED (Tombstones)")
  void testCompactWithDeletions() throws IOException {
    String tombstone = com.util.Constants.TOMBSTONE;

    // SSTable 1 (oldest)
    Map<String, String> data1 = new TreeMap<>();
    data1.put("keyA", "valA1");
    data1.put("keyB", "valB1");
    data1.put("keyC", "valC1");
    SSTable sstable1 = createSSTableFromMap(data1, tempDir);

    // SSTable 2 (middle)
    Map<String, String> data2 = new TreeMap<>();
    data2.put("keyB", "valB2");
    data2.put("keyD", "valD1");
    data2.put("keyC", tombstone);
    SSTable sstable2 = createSSTableFromMap(data2, tempDir);

    // SSTable 3 (newest)
    Map<String, String> data3 = new TreeMap<>();
    data3.put("keyA", tombstone);
    data3.put("keyE", "valE1");
    SSTable sstable3 = createSSTableFromMap(data3, tempDir);

    List<SSTable> sstablesToCompact = List.of(sstable1, sstable2, sstable3);

    Path outputSSTablePath = tempDir.resolve("compacted_deletions.sst");
    SSTable compactedSSTable = compactor.compact(sstablesToCompact, outputSSTablePath);

    Map<String, String> actual = readSSTableToMap(compactedSSTable);

    Map<String, String> expected = new TreeMap<>();
    expected.put("keyB", "valB2");
    expected.put("keyD", "valD1");
    expected.put("keyE", "valE1");

    assertEquals(expected, actual);
    assertTrue(Files.exists(compactedSSTable.getFilePath()));
  }
}
