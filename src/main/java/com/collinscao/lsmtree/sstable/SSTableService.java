package com.collinscao.lsmtree.sstable;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.memtable.MemtableService;
import com.util.Constants;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SSTableService {
  private static final int L0_THRESHOLD = 4;
  private static final int L1_THRESHOLD = 10;
  private final Manifest manifest;
  private final Compactor compactor;
  private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "LSM-Flush-Worker");
    t.setDaemon(true);
    return t;
  });
  private final ExecutorService compactExecutor = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "LSM-Compact-Worker");
    t.setDaemon(true);
    return t;
  });

  public SSTableService(Manifest manifest) {
    this.manifest = manifest;
    this.compactor = new Compactor();
  }

  public void submitFlushTask(MemtableService.ImmutableHolder holder, Runnable onComplete) {
    flushExecutor.submit(() -> {
      try {
        SSTable sstable = SSTable.createSSTableFromMemtable(holder.getMemtable(), manifest.getRootPath());
        manifest.applyFlush(0, sstable);
        if (holder.getWal() != null) {
          holder.getWal().delete();
        }
        onComplete.run();
        submitCompactionTask();
      } catch (Exception e) {
        System.err.println("Critical: Background flush failed!");
        e.printStackTrace();
      }
    });
  }

  private void submitCompactionTask() {
    compactExecutor.submit(() -> {
      try {
        checkAndCompactLevel(0);
      } catch (IOException e) {
        System.err.println("Critical: Background compaction failed!");
        e.printStackTrace();
      }
    });
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
      Path pathToDelete = table.getFilePath();
      Thread cleanupThread = new Thread(() -> {
        try {
          Thread.sleep(5000);
          Files.deleteIfExists(pathToDelete);
        } catch (Exception e) {
          System.err.println("Warning: Delayed deletion failed for " + pathToDelete);
        }
      });
      cleanupThread.setDaemon(true);
      cleanupThread.start();
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
      } else {
        for (SSTable table : levelList) {
          String val = table.get(key);
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

  public void stop() {
    flushExecutor.shutdown();
    compactExecutor.shutdown();
    try {
      if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        System.err.println("Warning: Flush tasks did not finish in time.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}