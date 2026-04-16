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

public class Manifest {
  private final Path rootPath;
  private final Map<Integer, List<SSTable>> levelMap;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

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

  public Path getRootPath() {
    return rootPath;
  }
}