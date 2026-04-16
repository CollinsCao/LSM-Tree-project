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


public class SSTable {
  private final Path filePath;
  private BloomFilter<String> bloomFilter;
  private final TreeMap<String, BlockInfo> blocks;
  private String maxKey;
  private String minKey;

  private static final int MAX_BLOCK_SIZE = 4000;

  public SSTable(Path filePath, BloomFilter bloomFilter, TreeMap<String, BlockInfo> blocks, String maxKey, String minKey) {
    this.filePath = filePath;
    this.bloomFilter = bloomFilter;
    this.blocks = blocks;
    this.maxKey = maxKey;
    this.minKey = minKey;
  }

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

  public static Path generateSSTablePath(Path rootPath) {
    String fileName = Constants.SSTABLE_PREFIX + System.nanoTime() + Constants.SSTABLE_FILE_EXTENSION;
    return rootPath.resolve(fileName);
  }


  public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException{
    return createSSTableFromMemtable(memtable, Path.of(Constants.DEFAULT_DATA_DIR).toAbsolutePath());
  }

  public static SSTable createSSTableFromMemtable(Memtable memtable, Path rootPath) throws IOException {
    return createSSTableFromIterator(memtable.iterator(), generateSSTablePath(rootPath));
  }

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
   * Search in current memtable
   * @param key The key db asks for.
   * @return Null if not found or val coresponding to key.
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
