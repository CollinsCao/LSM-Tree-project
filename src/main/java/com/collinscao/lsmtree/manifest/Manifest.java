package com.collinscao.lsmtree.manifest;

import com.collinscao.lsmtree.sstable.SSTable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.util.Constants;

/**
 * Manages the metadata and state of SSTable files across different levels in the LSM Tree.
 *
 * This class handles the persistence and recovery of database state, maintaining a mapping
 * of SSTable files organized by level. It provides thread-safe operations for adding and
 * removing SSTables during flush and compaction operations.
 */
public class Manifest {
  private final Path rootPath;
  private final Map<Integer, List<SSTable>> levelMap;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * Creates a new Manifest instance for the specified data directory.
   *
   * Initializes the manifest with the given data directory path and attempts to recover
   * the database state from existing manifest files if they exist.
   *
   * @param dataDir the directory path where database files are stored
   * @throws IOException if an I/O error occurs during initialization or recovery
   */
  public Manifest(String dataDir) throws IOException {
    this.rootPath = Path.of(dataDir).toAbsolutePath();
    this.levelMap = new HashMap<>();
    if (!Files.exists(rootPath)) Files.createDirectories(rootPath);

    recover();
  }

  private void recover() throws IOException {
    Path currentFilePath = rootPath.resolve(Constants.CURRENT_FILENAME);

    // New database.
    if (!Files.exists(currentFilePath)) {
      return;
    }
    // Existing database (Recovery)
    // 1. Read filename referenced by CURRENT
    // trim() to remove potential newlines from readString
    String manifestFileName = Files.readString(currentFilePath).trim();
    Path manifestPath = rootPath.resolve(manifestFileName);
    if (!Files.exists(manifestPath)) {
      throw new IOException("Manifest file pointed by CURRENT does not exist: " + manifestFileName);
    }

    try (InputStream is = Files.newInputStream(manifestPath);
        ObjectInputStream ois = new ObjectInputStream(is)) {
      Map<Integer, List<String>> diskData = (Map<Integer, List<String>>)ois.readObject();
      for (Map.Entry<Integer, List<String>> entry : diskData.entrySet()) {
        int level = entry.getKey();
        List<String> levelData = entry.getValue();
        List<SSTable> levelSstables = new CopyOnWriteArrayList<>();
        for (String partOfPath : levelData) {
          levelSstables.add(new SSTable(rootPath.resolve(partOfPath)));
        }
        this.levelMap.put(level, levelSstables);
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize manifest", e);
    }
  }

  private Map<Integer, List<String>> creatDiskDataSnapshot(){
    Map<Integer, List<String>> diskData = new HashMap<>();
    for (Map.Entry<Integer, List<SSTable>> entry : this.levelMap.entrySet()) {
      int level = entry.getKey();
      List<SSTable> levelSSTs = entry.getValue();
      List<String> levelData = new ArrayList<>();
      for (SSTable sst : levelSSTs) {
        levelData.add(String.valueOf(sst.getFilePath().getFileName()));
      }
      diskData.put(level, levelData);
    }
    return diskData;
  }

  private void persist(Map<Integer, List<String>> diskData) throws IOException {
    Path newManifestPath = generateMenifestPath();

    try (OutputStream os = Files.newOutputStream(newManifestPath);
        ObjectOutputStream oos = new ObjectOutputStream(os)) {
      oos.writeObject(diskData);
    }

    Path pathOfCurrentManifestFile = rootPath.resolve(Constants.CURRENT_FILENAME);
    Files.writeString(pathOfCurrentManifestFile, newManifestPath.getFileName().toString());
  }

  /**
   * Returns a snapshot of the current level mapping.
   *
   * Creates a thread-safe copy of the current SSTable organization across all levels.
   * The returned map is a snapshot and will not reflect subsequent changes.
   *
   * @return a map where keys are level numbers and values are lists of SSTables at that level
   */
  public Map<Integer, List<SSTable>> getLevelSnapshot() {
    rwLock.readLock().lock();
    try {
      Map<Integer, List<SSTable>> snapShot = new HashMap<>();
      for (Map.Entry<Integer, List<SSTable>> entry : this.levelMap.entrySet()) {
        snapShot.put(entry.getKey(), new ArrayList<>(entry.getValue()));
      }
      return snapShot;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Applies a flush operation by adding an SSTable to the specified level.
   *
   * This method is called when memtable data is flushed to disk as an SSTable.
   * It atomically updates the manifest state and persists the changes.
   *
   * @param level the level where the SSTable should be added
   * @param sstable the SSTable to add to the manifest
   * @throws IOException if an I/O error occurs during persistence
   */
  public void applyFlush(int level, SSTable sstable) throws IOException {
    Map<Integer, List<String>> diskDataSnapshot;
    rwLock.writeLock().lock();
    try {
      innerAdd(level, sstable);
      diskDataSnapshot = creatDiskDataSnapshot();
    } finally {
      rwLock.writeLock().unlock();
    }
    persist(diskDataSnapshot);
  }

  /**
   * Applies a compaction operation by removing old SSTables and adding a new one.
   *
   * This method is called during compaction when multiple SSTables from a source level
   * are merged into a single SSTable at a target level. It atomically updates the manifest
   * state and persists the changes.
   *
   * @param sourceLevel the level from which SSTables are being removed
   * @param oldTables the list of SSTables to remove from the source level
   * @param targetLevel the level where the new SSTable should be added
   * @param newTable the new SSTable to add to the target level
   * @throws IOException if an I/O error occurs during persistence
   */
  public void applyCompact(int sourceLevel, List<SSTable> oldTables,
      int targetLevel, SSTable newTable) throws IOException {
    Map<Integer, List<String>> diskDataSnapshot;
    rwLock.writeLock().lock();
    try {
      innerRemove(sourceLevel, oldTables);
      innerAdd(targetLevel, newTable);
      diskDataSnapshot = creatDiskDataSnapshot();
    } finally {
      rwLock.writeLock().unlock();
    }
    persist(diskDataSnapshot);
  }

  private void innerAdd(int level, SSTable sstable) {
    levelMap.putIfAbsent(level, new CopyOnWriteArrayList<>());
    levelMap.get(level).add(sstable);
  }

  private void innerRemove(int level, List<SSTable> sstables) throws IOException {
    List<SSTable> levelTables = levelMap.get(level);
    if (levelTables == null || sstables == null || sstables.isEmpty()) {
      return;
    }
    levelTables.removeAll(sstables);
  }

  /**
   * Returns a list of SSTables at the specified level.
   *
   * Provides a thread-safe snapshot of all SSTables currently stored at the given level.
   * Returns an empty list if the level contains no SSTables.
   *
   * @param level the level number to query
   * @return a list of SSTables at the specified level, or an empty list if none exist
   */
  public List<SSTable> getSSTable(int level) {
    rwLock.readLock().lock();
    try {
      List<SSTable> levelList = levelMap.get(level);
      if (levelList == null) {
        return new ArrayList<>();
      }
      return new ArrayList<>(levelList);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private Path generateMenifestPath() {
    String fileName = Constants.MANIFEST_PREFIX + System.nanoTime();
    return rootPath.resolve(fileName);
  }

  /**
   * Returns the root directory path for database files.
   *
   * @return the absolute path to the data directory
   */
  public Path getRootPath() {
    return rootPath;
  }
}