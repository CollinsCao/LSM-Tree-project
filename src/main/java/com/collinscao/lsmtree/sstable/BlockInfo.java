package com.collinscao.lsmtree.sstable;

public class BlockInfo {
  long offset;
  long size;

  public BlockInfo(long offset, long size) {
    this.offset = offset;
    this.size = size;
  }
}
