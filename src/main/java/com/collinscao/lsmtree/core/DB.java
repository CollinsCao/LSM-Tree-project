package com.collinscao.lsmtree.core;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.sstable.SSTableService;
import com.collinscao.lsmtree.memtable.MemtableService;
import com.util.Constants;
import java.io.IOException;

/**
 * LSM Tree database implementation for key-value storage and retrieval.
 *
 * <p>This class integrates a two-tier storage architecture consisting of an in-memory table
 * (Memtable) and sorted string tables (SSTables) for efficient read and write operations.
 * It implements {@link AutoCloseable} for proper resource management with try-with-resources.</p>
 */
public class DB implements AutoCloseable {
    /** Manifest for managing database state and metadata. */
    private final Manifest manifest;
    /** In-memory table service for write and read operations. */
    private final MemtableService memtableService;
    /** Sorted string table service for persistent storage. */
    private final SSTableService sstableService;

    /**
     * Creates a database instance using the default data directory.
     *
     * @throws IOException if an I/O error occurs during initialization
     */
    public DB() throws IOException {
      this(Constants.DEFAULT_DATA_DIR);
    }

    /**
     * Creates a database instance using the specified data directory.
     *
     * @param dataDir the directory path where data files are stored
     * @throws IOException if an I/O error occurs during initialization
     */
    public DB(String dataDir) throws IOException {
      manifest = new Manifest(dataDir);
      sstableService = new SSTableService(manifest);
      memtableService = new MemtableService(manifest, sstableService);
    }

    /**
     * Stores a key-value pair in the database.
     *
     * @param key the key to store
     * @param value the value associated with the key
     * @throws IOException if an I/O error occurs during storage
     */
    public void put(String key, String value) throws IOException {
      memtableService.put(key, value);
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * <p>The lookup first checks the memtable, then the SSTables if needed.
     * Returns null if the key does not exist or has been deleted (tombstone marker).</p>
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if not found or deleted
     */
    public String get(String key) {
      String value = memtableService.get(key);
      if (value == null) {
        value = sstableService.get(key);
      }
      return (value == null || value.equals(Constants.TOMBSTONE)) ? null : value;
    }

    /**
     * Deletes the value associated with the specified key.
     *
     * <p>Deletion is implemented by storing a tombstone marker rather than removing the data.</p>
     *
     * @param key the key to delete
     * @throws IOException if an I/O error occurs during deletion
     */
    public void remove(String key) throws IOException {
      memtableService.put(key, Constants.TOMBSTONE);
    }

    /**
     * Closes the database and releases all resources.
     *
     * <p>Ensures that all pending data is persisted to storage.</p>
     *
     * @throws IOException if an I/O error occurs during shutdown
     */
    @Override
    public void close() throws IOException {
      memtableService.close();
      sstableService.stop();
    }
  }