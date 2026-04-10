package com.collinscao.lsmtree.sstable;


import com.collinscao.memtable.Memtable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import com.util.IOUtils;

public class SSTable {
  private final Path filePath;
  //private final BloomFilterUtil bloomFilterUtil;
  private final TreeMap<String, BlockInfo> blocks;
  private String maxKey;
  private String minKey;

  private static final int MAX_BLOCK_SIZE = 4000;

  public SSTable(Path filePath) throws IOException {
    this.filePath = filePath;
    this.blocks = new TreeMap<>();
    this.maxKey = null;
    this.minKey = null;
    init();
  }

  public SSTable(Path filePath, TreeMap<String, BlockInfo> blocks, String maxKey, String minKey) {
    this.filePath = filePath;
    this.blocks = blocks;
    this.maxKey = maxKey;
    this.minKey = minKey;
  }

  private void init() throws IOException {
    long startOfBlock = 0L;
    long lenOfBlock = 0L;
    String firstKeyInBlock = null;

    try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(filePath), "r")) {
      while (raf.getFilePointer() <raf.length()) {
        long startOfCurEntry = raf.getFilePointer();
        String key = IOUtils.readNextString(raf);
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


  public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException{
    return createSSTableFromMemtable(memtable, Path.of("./data"));
  }

  public static SSTable createSSTableFromMemtable(Memtable memtable, Path dirtory) throws IOException {
    File folder = dirtory.toFile();
    if (!folder.exists()) {
      folder.mkdirs();
    }
    Path filePath = dirtory.resolve("sstable_" + System.nanoTime() + ".sst");
    // TODO: BloomFilter
    TreeMap<String, BlockInfo> blocks = new TreeMap<>();//<firstKey, blockInfor>
    String minKey = null;
    String maxKey = null;

    try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(filePath), "rw"))  {
      Iterator<Entry<String, String>> iterator = memtable.iterator();
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
    return new SSTable(filePath, blocks, maxKey, minKey);
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
}
