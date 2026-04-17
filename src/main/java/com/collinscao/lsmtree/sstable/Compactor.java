package com.collinscao.lsmtree.sstable;


import com.util.Constants;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Handles compaction operations for merging multiple SSTables into a single SSTable.
 *
 * This class provides functionality to merge multiple SSTable files while handling
 * key conflicts and tombstone markers. It uses a priority queue-based merge iterator
 * to efficiently combine sorted data from multiple sources.
 */
public class Compactor{
  /**
   * Represents an element in the merge priority queue.
   *
   * This class holds a key-value entry along with the index of the source SSTable
   * it came from, enabling proper conflict resolution during merging.
   */
  private static class MergeElement {
    /** The index of the source SSTable this entry came from. */
    int indexOfSourceSSTable;
    /** The key-value entry from the SSTable. */
    Entry<String, String> entry;

    /**
     * Creates a new MergeElement with the specified source index and entry.
     *
     * @param indexOfSourceSSTable the index of the source SSTable
     * @param entry the key-value entry
     */
    public MergeElement(int indexOfSourceSSTable, Entry<String, String> entry) {
      this.indexOfSourceSSTable = indexOfSourceSSTable;
      this.entry = entry;
    }
  }

  /**
   * Compacts multiple SSTables into a single SSTable file.
   *
   * This method merges the provided list of SSTables using a priority queue-based
   * merge iterator that handles key conflicts and filters out tombstone entries.
   * The resulting SSTable is written to the specified output path.
   *
   * @param listOfSStable the list of SSTables to compact
   * @param outFilePath the path where the compacted SSTable will be written
   * @return the newly created compacted SSTable
   * @throws IOException if an I/O error occurs during compaction
   */
  public SSTable compact(List<SSTable> listOfSStable, Path outFilePath) throws IOException {
    List<SSTableIterator> list = new ArrayList<>();
    try {
      for (SSTable ssTable : listOfSStable) {
        list.add(new SSTableIterator(ssTable));
      }
      Iterator<Entry<String, String>> iterators = new MergedIterator(list);
      return SSTable.createSSTableFromIterator(iterators, outFilePath);
    } finally {
      for (SSTableIterator it : list) {
        try {
          it.close();
        } catch (IOException e) {
          System.err.println("Error closing SSTableIterator: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Iterator that merges multiple SSTable iterators in sorted order.
   *
   * This iterator uses a priority queue to merge entries from multiple SSTable iterators,
   * handling key conflicts by preferring entries from higher-indexed SSTables (more recent)
   * and filtering out tombstone entries.
   */
  private static class MergedIterator implements Iterator<Entry<String, String>> {
    private final List<SSTableIterator> list;
    private final PriorityQueue<MergeElement> heap;
    private Entry<String, String> nextEntry;

    /**
     * Creates a new MergedIterator for the given list of SSTable iterators.
     *
     * Initializes the priority queue with the first entry from each iterator
     * and advances to the first valid (non-tombstone) entry.
     *
     * @param list the list of SSTable iterators to merge
     */
    public MergedIterator(List<SSTableIterator> list) {
      this.list = list;
      this.heap = new PriorityQueue<>((a, b) -> {
        int res = a.entry.getKey().compareTo(b.entry.getKey());
        if (res == 0) {return Integer.compare(b.indexOfSourceSSTable, a.indexOfSourceSSTable);}
        return res;
      });
      this.nextEntry = null;

      for (int i = 0; i < list.size(); i++) {
        if (list.get(i).hasNext()) {
          heap.offer(new MergeElement(i, list.get(i).next()));
        }
      }
      advance();
    }

    /**
     * Advances to the next valid entry in the merged iteration.
     *
     * This method processes entries from the priority queue, handling key conflicts
     * by preferring entries from higher-indexed SSTables and skipping tombstone entries.
     * It ensures that only the most recent valid entry for each key is returned.
     */
    public void advance() {
      while (true) {
        if (heap.isEmpty()) {
          nextEntry = null;
          return;
        }
        MergeElement currentMin = heap.poll();
        int indexOfSourceSSTable = currentMin.indexOfSourceSSTable;
        nextEntry = currentMin.entry;
        String curKey = nextEntry.getKey();

        if (list.get(indexOfSourceSSTable).hasNext()) {
          heap.offer(new MergeElement(indexOfSourceSSTable, list.get(indexOfSourceSSTable).next()));
        }

        while (!heap.isEmpty() && heap.peek().entry.getKey().equals(curKey)) {
          int duplicateTableIndex = heap.poll().indexOfSourceSSTable;
          if (list.get(duplicateTableIndex).hasNext()) {
            heap.offer(new MergeElement(duplicateTableIndex, list.get(duplicateTableIndex).next()));
          }
        }
        
        if (nextEntry.getValue().equals(Constants.TOMBSTONE)) {
          continue;
        }
        break;
      }
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Entry<String, String> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Entry<String, String> result = nextEntry;
      advance();
      return result;
    }
  }
}