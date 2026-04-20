package com.collinscao.lsmtree.sstable;


import com.collinscao.lsmtree.memtable.Memtable;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.util.Constants;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import com.util.IOUtils;


/**
 * Represents a Sorted String Table (SSTable) for persistent key-value storage.
 *
 * This class manages SSTable files that store sorted key-value pairs on disk.
 * It includes a Bloom filter for efficient key existence checks, block-based
 * indexing for fast lookups, and metadata about key ranges.
 */
public class SSTable {
  private final Path filePath;
  private BloomFilter<String> bloomFilter;
  private final TreeMap<String, BlockInfo> blocks;
  private String maxKey;
  private String minKey;

  private static final int MAX_BLOCK_SIZE = 4000;

  /**
   * Creates a new SSTable instance with the specified parameters.
   *
   * This constructor is used when creating an SSTable from existing data
   * without needing to read from disk.
   *
   * @param filePath the path to the SSTable file
   * @param bloomFilter the Bloom filter for key existence checks
   * @param blocks the block index mapping first keys to block information
   * @param maxKey the maximum key in this SSTable
   * @param minKey the minimum key in this SSTable
   */
  public SSTable(Path filePath, BloomFilter bloomFilter, TreeMap<String, BlockInfo> blocks, String maxKey, String minKey) {
    this.filePath = filePath;
    this.bloomFilter = bloomFilter;
    this.blocks = blocks;
    this.maxKey = maxKey;
    this.minKey = minKey;
  }

  /**
   * Creates a new SSTable instance by loading an existing file from disk.
   *
   * This constructor reads the SSTable file, initializes the Bloom filter,
   * builds the block index, and determines the key range.
   *
   * @param filePath the path to the existing SSTable file
   * @throws IOException if an I/O error occurs while reading the file
   */
  public SSTable(Path filePath) throws IOException {
    this.filePath = filePath;
    this.blocks = new TreeMap<>();
    this.maxKey = null;
    this.minKey = null;
    init();
  }

  private void init() throws IOException {
    long startOfBlock = 0L;
    long lenOfBlock = 0L;
    String firstKeyInBlock = null;
    this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Constants.EXPECTED_INSERTIONS, Constants.FALSE_POSITIVE_PROBABILITY);

    try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(filePath), "r")) {
      while (raf.getFilePointer() <raf.length()) {
        long startOfCurEntry = raf.getFilePointer();
        String key = IOUtils.readNextString(raf);
        bloomFilter.put(key);
        int lenOfValue = raf.readInt();
        raf.skipBytes(lenOfValue);
        long lenOfEntry = raf.getFilePointer() - startOfCurEntry;

        if (lenOfEntry + lenOfBlock > MAX_BLOCK_SIZE && firstKeyInBlock != null) {
          // Record the block.
          this.blocks.put(firstKeyInBlock, new BlockInfo(startOfBlock, lenOfBlock));
          // Update for a new block.
          startOfBlock = startOfCurEntry;
          lenOfBlock = 0L;
          firstKeyInBlock = null;
        }

        // Include current entry into block
        if (firstKeyInBlock == null) {
          firstKeyInBlock = key;
        }

        lenOfBlock += lenOfEntry;
        minKey = (minKey == null) ? key : minKey;
        maxKey = key;
      }

      if (firstKeyInBlock != null) {
        this.blocks.put(firstKeyInBlock, new BlockInfo(startOfBlock, lenOfBlock));
      }
    }
  }


  /**
   * Generates a unique file path for a new SSTable.
   *
   * Creates a path using the SSTable prefix, current nanosecond timestamp,
   * and file extension to ensure uniqueness.
   *
   * @param rootPath the root directory for SSTable files
   * @return a unique path for the new SSTable file
   */
  public static Path generateSSTablePath(Path rootPath) {
    String fileName = Constants.SSTABLE_PREFIX + System.nanoTime() + Constants.SSTABLE_FILE_EXTENSION;
    return rootPath.resolve(fileName);
  }

//  public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException{
//    return createSSTableFromMemtable(memtable, Path.of("./data"));
//  }

  /**
   * Creates an SSTable from a memtable.
   *
   * Flushes the contents of the memtable to a new SSTable file on disk.
   *
   * @param memtable the memtable to flush
   * @param rootPath the root directory for the SSTable file
   * @return the newly created SSTable
   * @throws IOException if an I/O error occurs during creation
   */
  public static SSTable createSSTableFromMemtable(Memtable memtable, Path rootPath) throws IOException {
    return createSSTableFromIterator(memtable.iterator(), generateSSTablePath(rootPath));
  }

  /**
   * Creates an SSTable from an iterator of key-value entries.
   *
   * Writes the entries from the iterator to a new SSTable file, building
   * the Bloom filter and block index during the process.
   *
   * @param iterator the iterator providing key-value entries in sorted order
   * @param filePath the path where the SSTable file will be created
   * @return the newly created SSTable
   * @throws IOException if an I/O error occurs during creation
   */
  public static SSTable createSSTableFromIterator(Iterator<Entry<String, String>> iterator, Path filePath) throws IOException {
    Path folder = filePath.getParent();
    if (folder != null) {
      Files.createDirectories(folder);
    }

    BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8),
        Constants.EXPECTED_INSERTIONS, Constants.FALSE_POSITIVE_PROBABILITY);

    TreeMap<String, BlockInfo> blocks = new TreeMap<>();//<firstKey, blockInfor>
    String minKey = null;
    String maxKey = null;

    try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(filePath), "rw"))  {
      long lenOfBlock = 0L;
      long startOfBlock = 0L;
      String firstKeyInBlock = null;

      while (iterator.hasNext()) {
        Entry<String, String> entry = iterator.next();
        String key = entry.getKey();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
        long lenOfEntry = 4 + keyBytes.length + 4 + valueBytes.length;

        if (lenOfBlock + lenOfEntry > MAX_BLOCK_SIZE && firstKeyInBlock != null) {
          // out of block capacity: store this block
          blocks.put(firstKeyInBlock, new BlockInfo(startOfBlock, lenOfBlock));
          // update for new block
          startOfBlock = raf.getFilePointer();
          firstKeyInBlock = null;
          lenOfBlock = 0L;
        }

        if (firstKeyInBlock == null) {
          firstKeyInBlock = key;
        }

        raf.writeInt(keyBytes.length);
        raf.write(keyBytes);
        raf.writeInt(valueBytes.length);
        raf.write(valueBytes);
        bloomFilter.put(key);

        if (minKey == null) {
          minKey = key;
        }
        maxKey = key;
        lenOfBlock += lenOfEntry;
      }

      if (firstKeyInBlock != null) {
        // store the last block
        blocks.put(firstKeyInBlock, new BlockInfo(startOfBlock, lenOfBlock));
      }
    }
    return new SSTable(filePath, bloomFilter, blocks, maxKey, minKey);
  }

  /**
   * Retrieves the value associated with the specified key from this SSTable.
   *
   * Performs a multi-stage lookup process:
   * 1. Checks if the key is within the SSTable's key range
   * 2. Uses the Bloom filter for fast existence check
   * 3. Locates the appropriate block using the block index
   * 4. Performs binary search within the block data
   *
   * @param key the key to look up
   * @return the value associated with the key, or null if not found
   */
  public String get(String key) {
    if (key.compareTo(maxKey) > 0 || key.compareTo(minKey) < 0) {
      // not in current sstable.
      return null;
    }

    if (!bloomFilter.mightContain(key)) {
      return null;
    }

    Map.Entry<String, BlockInfo> entry = blocks.floorEntry(key);
    if (entry == null) {
      return null;
    }
    BlockInfo infoOfTargetBlock = entry.getValue();

    // try-with-resources: responsible for opening file on disk.
    try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(filePath), "r")) {
      raf.seek(infoOfTargetBlock.offset);
      byte[] blockData = new byte[(int)infoOfTargetBlock.size];
      raf.readFully(blockData);// Load data from disk into memory buffer.

      // Wraps the byte array as an InputStream for easier access.
      try (ByteArrayInputStream bais = new ByteArrayInputStream(blockData);
          DataInputStream dis = new DataInputStream(bais)) {
        while (dis.available() > 0) {
          int lenOfKey = dis.readInt();
          byte[] keyBytes = new byte[lenOfKey];
          dis.readFully(keyBytes);
          String curKey = new String(keyBytes, StandardCharsets.UTF_8);

          int lenOfValue = dis.readInt();
          byte[] valueBytes = new byte[lenOfValue];
          dis.readFully(valueBytes);

          if (curKey.equals(key)) {
            return IOUtils.deserializeValue(valueBytes);
          }
          if (curKey.compareTo(key) > 0) {
            return null;
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading sstable file" + filePath, e);
    }
    return null;
  }

  /**
   * Returns the file path of this SSTable.
   *
   * @return the path to the SSTable file on disk
   */
  public Path getFilePath() {
    return filePath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SSTable sstable = (SSTable) o;
    return java.util.Objects.equals(filePath, sstable.filePath);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(filePath);
  }
}
