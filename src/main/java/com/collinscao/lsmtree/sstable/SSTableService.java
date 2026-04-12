package com.collinscao.lsmtree.sstable;

import com.collinscao.lsmtree.manifest.Manifest;
import java.util.List;

public class SSTableService {
  private Manifest manifest;

  public SSTableService(Manifest manifest) {
    this.manifest = manifest;
  }

  public String get(String key) {
    List<SSTable> levelList = manifest.getSSTable(0);
    for (SSTable level : levelList) {
      String value = level.get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }
}
