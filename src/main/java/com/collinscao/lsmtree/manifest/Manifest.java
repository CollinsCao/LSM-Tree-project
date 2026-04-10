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
import com.util.Constants;

public class Manifest {
  private final String dataDir;
  private final Map<Integer, List<SSTable>> levelMap;

  public Manifest(String dataDir) throws IOException {
    this.dataDir = dataDir;
    this.levelMap = new HashMap<Integer, List<SSTable>>();

    Path path = Path.of(dataDir);
    if (!Files.exists(path)) Files.createDirectories(path);

    recover();
  }

  private void recover() throws IOException {
    Path currentFilePath = Path.of(dataDir, Constants.CURRENT_FILENAME);

    // New database.
    if (!Files.exists(currentFilePath)) {
      return;
    }
    // Existing database (Recovery)
    // 1. Read filename referenced by CURRENT
    // trim() to remove potential newlines from readString
    String manifestFileName = Files.readString(currentFilePath).trim();
    Path manifestPath = Path.of(dataDir, manifestFileName);
    if (!Files.exists(manifestPath)) {
      throw new IOException("Manifest file pointed by CURRENT does not exist: " + manifestFileName);
    }

    try (InputStream is = Files.newInputStream(manifestPath);
        ObjectInputStream ois = new ObjectInputStream(is)) {
      @SuppressWarnings("unchecked")
      Map<Integer, List<String>> diskData = (Map<Integer, List<String>>)ois.readObject();
      for (Map.Entry<Integer, List<String>> entry : diskData.entrySet()) {
        int level = entry.getKey();
        List<String> levelData = entry.getValue();
        List<SSTable> levelSstables = new ArrayList<>();
        for (String partOfPath : levelData) {
          levelSstables.add(new SSTable(Path.of(dataDir, partOfPath)));
        }
        this.levelMap.put(level, levelSstables);
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize manifest", e);
    }
  }

  private String generateNextMenifestName() {
    return Constants.MANIFEST_FILENAME + "-" + System.nanoTime();
  }

  private synchronized void persist() throws IOException {
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

    String newManifestFileName = generateNextMenifestName();
    Path newManifestPath = Path.of(dataDir, newManifestFileName);

    try (OutputStream os = Files.newOutputStream(newManifestPath);
        ObjectOutputStream oos = new ObjectOutputStream(os)) {
      oos.writeObject(diskData);
    }

    Path pathOfCurrentManifestFile = Path.of(dataDir, Constants.CURRENT_FILENAME);
    Files.writeString(pathOfCurrentManifestFile, newManifestFileName);
  }

  public void addSSTable(int level, SSTable sstable) throws IOException {
    levelMap.putIfAbsent(level, new ArrayList<>());
    levelMap.get(level).add(0, sstable);
    persist();
  }

  public List<SSTable> getSSTable(int level) {
    List<SSTable> levelList = levelMap.get(level);
    if (levelList == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(levelList);
  }
}