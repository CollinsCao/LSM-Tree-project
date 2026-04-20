package com.collinscao.lsmtree.sstable;

/**
 * Represents metadata for a data block in an SSTable.
 *
 * This class stores the offset and size information for a block of data
 * within an SSTable file, enabling efficient data retrieval and indexing.
 */
public class BlockInfo {
  /** The byte offset of the block within the SSTable file. */
  long offset;
  /** The size of the block in bytes. */
  long size;

  /**
   * Creates a new BlockInfo instance with the specified offset and size.
   *
   * @param offset the byte offset of the block within the SSTable file
   * @param size the size of the block in bytes
   */
  public BlockInfo(long offset, long size) {
    this.offset = offset;
    this.size = size;
  }
}
