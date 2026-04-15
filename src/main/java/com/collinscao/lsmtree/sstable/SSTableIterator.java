package com.collinscao.lsmtree.sstable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import com.util.IOUtils;

public class SSTableIterator implements Iterator<Entry>, AutoCloseable {
  private final RandomAccessFile raf;
  private long fileSize;

  public SSTableIterator(SSTable sstable) throws IOException {
    this.raf = new RandomAccessFile(String.valueOf(sstable.getFilePath()), "r");
    this.fileSize = raf.length();
  }

  @Override
  public boolean hasNext() {
    try {
      return raf.getFilePointer() < fileSize;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public Map.Entry<String, String> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      String key = IOUtils.readNextString(raf);
      int lenOfValue = raf.readInt();
      byte[] buf = new byte[lenOfValue];
      raf.readFully(buf);
      return new AbstractMap.SimpleEntry<String, String>(key, new String(buf));
    } catch (IOException e) {
      throw new RuntimeException("Error reading from SSTable during iteration", e);
    }
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }
}