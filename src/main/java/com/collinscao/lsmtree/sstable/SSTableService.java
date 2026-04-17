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

/**
 * Service for managing SSTable operations including flushing and compaction.
 *
 * This class coordinates the background processing of memtable flushes to SSTables
 * and manages the compaction process across different levels. It uses separate
 * thread pools for flush and compaction operations to ensure efficient resource usage.
 */
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

  /**
   * Creates a new SSTableService with the specified manifest.
   *
   * Initializes the service with separate thread pools for flush and compaction
   * operations, and creates a compactor instance for merging SSTables.
   *
   * @param manifest the manifest for managing SSTable metadata
   */
  public SSTableService(Manifest manifest) {
    this.manifest = manifest;
    this.compactor = new Compactor();
  }

  /**
   * Submits a background task to flush an immutable memtable to an SSTable.
   *
   * This method creates an SSTable from the memtable data, updates the manifest,
   * deletes the associated WAL file, and triggers compaction if needed.
   *
   * @param holder the immutable memtable and WAL holder to flush
   * @param onComplete callback to run after successful flush
   */
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

  /**
   * Retrieves the value associated with the specified key from SSTables.
   *
   * Searches through all levels of SSTables in order, starting from level 0
   * (most recent) to higher levels. For level 0, searches from newest to oldest
   * SSTable. For other levels, searches in any order since they are sorted.
   *
   * @param key the key to look up
   * @return the value associated with the key, or null if not found
   */
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

  /**
   * Stops the SSTable service and shuts down background threads.
   *
   * Initiates shutdown of both flush and compaction executors and waits
   * up to 30 seconds for flush tasks to complete. Compaction tasks may
   * continue running in the background.
   */
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