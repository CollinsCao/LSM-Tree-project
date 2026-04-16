package com.collinscao.lsmtree.sstable;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.memtable.Memtable;
import com.util.Constants;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SSTableService {
  private static final int L0_THRESHOLD = 4;
  private static final int L1_THRESHOLD = 10;
  private final Manifest manifest;
  private final Compactor compactor;

  public SSTableService(Manifest manifest) {
    this.manifest = manifest;
    this.compactor = new Compactor();
  }

  public void flush(Memtable memTable) throws IOException {
    if (memTable.getSize() == 0) {return;}
    SSTable sstable = SSTable.createSSTableFromMemtable(memTable, manifest.getRootPath());
    this.manifest.applyFlush(0, sstable);
    checkAndCompactLevel(0);
  }


  private void checkAndCompactLevel(int level) throws IOException {
    List<SSTable> tables = manifest.getSSTable(level);
    if (!needsCompaction(level, tables)) {
      return;
    }
    List<SSTable> tablesToCompact = new ArrayList<>(tables);
    int nextLevel = level + 1;
    Path newFilePath = SSTable.generateSSTablePath(manifest.getRootPath());
    SSTable newSSTable = compactor.compact(tablesToCompact, newFilePath);
    manifest.applyCompact(level, tablesToCompact, nextLevel, newSSTable);
    for (SSTable table : tablesToCompact) {
      Files.deleteIfExists(table.getFilePath());
    }
    checkAndCompactLevel(nextLevel);
  }

  private boolean needsCompaction(int level, List<SSTable> tables) {
    if (level == 0) {
      return tables.size() >= L0_THRESHOLD;
    } else if (level == 1) {
      return tables.size() >= L1_THRESHOLD;
    }
    return false;
  }



  public String get(String key) {
    for (int i = 0; i < Constants.MAX_LEVEL; i++) {
      List<SSTable> levelList = manifest.getSSTable(i);
      if (i == 0) {
        for (int j = levelList.size() - 1; j >= 0; j--) {
          String val = levelList.get(j).get(key);
          if (val != null) {
            return handleTombstone(val);
          }
        }
      }
    }
    return null;
  }

    private String handleTombstone(String val) {
      return val.equals(Constants.TOMBSTONE) ? null : val;
    }
}
