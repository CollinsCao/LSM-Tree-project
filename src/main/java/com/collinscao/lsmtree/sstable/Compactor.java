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

public class Compactor{
  private static class MergeElement {
    int indexOfSourceSSTable;
    Entry<String, String> entry;

    public MergeElement(int indexOfSourceSSTable, Entry<String, String> entry) {
      this.indexOfSourceSSTable = indexOfSourceSSTable;
      this.entry = entry;
    }
  }

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

  private static class MergedIterator implements Iterator<Entry<String, String>> {
    private final List<SSTableIterator> list;
    private final PriorityQueue<MergeElement> heap;
    private Entry<String, String> nextEntry;

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