package com.collinscao.lsmtree.core;

import com.collinscao.memtable.Memtable;
import com.util.Constants;

public class DB {
  private Memtable memtable;

  public DB() {
    memtable = new Memtable();
  }
  public void put(String key, String value) {
    memtable.put(key, value);
  }

  public String get(String key) {
    String value = memtable.get(key);
    return (value.equals(Constants.TOMBSTONE) ? null : value);
  }

  public void remove(String key) {
    memtable.put(key, Constants.TOMBSTONE);
  }

  public void close() {
    // TODO: finish.
  }
}