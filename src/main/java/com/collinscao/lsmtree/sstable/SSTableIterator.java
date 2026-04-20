package com.collinscao.lsmtree.sstable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import com.util.IOUtils;

/**
 * Iterator for traversing entries in an SSTable file.
 *
 * This class provides sequential access to key-value pairs stored in an SSTable,
 * implementing both Iterator and AutoCloseable interfaces for safe resource management.
 * It reads entries directly from the SSTable file on disk.
 */
public class SSTableIterator implements Iterator<Entry<String ,String>>, AutoCloseable {
  private final RandomAccessFile raf;
  private long fileSize;

  /**
   * Creates a new iterator for the specified SSTable.
   *
   * Opens a RandomAccessFile for reading the SSTable file and initializes
   * the iterator to start from the beginning of the file.
   *
   * @param sstable the SSTable to iterate over
   * @throws IOException if an I/O error occurs while opening the file
   */
  public SSTableIterator(SSTable sstable) throws IOException {
    this.raf = new RandomAccessFile(String.valueOf(sstable.getFilePath()), "r");
    this.fileSize = raf.length();
  }

  /**
   * Returns whether there are more entries to iterate over.
   *
   * Checks if the current file pointer is before the end of the file.
   * Returns false if an I/O error occurs during the check.
   *
   * @return true if there are more entries, false otherwise
   */
  @Override
  public boolean hasNext() {
    try {
      return raf.getFilePointer() < fileSize;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Returns the next key-value entry in the iteration.
   *
   * Reads the next entry from the SSTable file, parsing the key and value
   * according to the SSTable format. Throws NoSuchElementException if no
   * more entries are available.
   *
   * @return the next key-value entry
   * @throws NoSuchElementException if no more entries are available
   * @throws RuntimeException if an I/O error occurs during reading
   */
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

  /**
   * Closes the iterator and releases associated resources.
   *
   * Closes the underlying RandomAccessFile to free system resources.
   * This method should be called when the iterator is no longer needed.
   *
   * @throws IOException if an I/O error occurs while closing the file
   */
  @Override
  public void close() throws IOException {
    raf.close();
  }
}