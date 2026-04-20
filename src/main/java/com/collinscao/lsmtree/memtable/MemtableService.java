package com.collinscao.lsmtree.memtable;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.sstable.SSTableService;
import com.collinscao.lsmtree.memtable.Memtable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import com.util.Constants;
import com.util.WAL;

/**
 * Manages the memtable and write-ahead log (WAL) operations in the LSM Tree.
 *
 * This service handles in-memory data storage, WAL persistence for durability,
 * and coordinates with the SSTable service for flushing immutable memtables to disk.
 * It provides thread-safe operations for put and get operations.
 */
public class MemtableService {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicBoolean isRotating = new AtomicBoolean(false);

    private final Manifest manifest;
    private final SSTableService sstableService;
    private volatile Memtable activeMemtable;
    private volatile WAL activeWal;
    private final Path rootPath;
    private volatile CopyOnWriteArrayList<ImmutableHolder> immutableMemtables;

    /**
     * Holds an immutable memtable and its associated WAL for background flushing.
     *
     * This class encapsulates a memtable that has been made immutable (read-only)
     * along with its corresponding WAL file, allowing them to be processed
     * asynchronously by the SSTable service.
     */
    public static class ImmutableHolder {

        private final Memtable memtable;
        private final WAL wal;

        /**
         * Creates a new holder for an immutable memtable and WAL pair.
         *
         * @param memtable the immutable memtable
         * @param wal the associated WAL file, may be null for recovered data
         */
        public ImmutableHolder(Memtable memtable, WAL wal) {
            this.memtable = memtable;
            this.wal = wal;
        }

        /**
         * Returns the immutable memtable.
         *
         * @return the memtable instance
         */
        public Memtable getMemtable() {
            return memtable;
        }

        /**
         * Returns the associated WAL file.
         *
         * @return the WAL instance, may be null
         */
        public WAL getWal() {
            return wal;
        }
    }

    /**
     * Creates a new MemtableService with the specified manifest and SSTable service.
     *
     * Initializes the service by recovering any existing WAL files and setting up
     * the active memtable and WAL. If recovered data exists, it schedules a flush
     * task to persist the recovered memtable to disk.
     *
     * @param manifest the manifest for managing SSTable metadata
     * @param sstableService the service for handling SSTable operations
     * @throws IOException if an I/O error occurs during initialization or recovery
     */
    public MemtableService(Manifest manifest, SSTableService sstableService) throws IOException {
        this.manifest = manifest;
        this.sstableService = sstableService;
        this.activeMemtable = new Memtable();
        this.rootPath = manifest.getRootPath();
        this.immutableMemtables = new CopyOnWriteArrayList<>();

        List<Path> oldWalFiles = new ArrayList<>();
        try (Stream<Path> stream = Files.list(rootPath)) {
            stream.filter(path -> path.getFileName().toString().startsWith(Constants.WAL_PREFIX))
                    .filter(path -> path.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION))
                    .forEach(oldWalFiles::add);
        }

        WAL.recoverAll(rootPath, activeMemtable);
        if (activeMemtable.getSize() > 0) {
            ImmutableHolder recoveredHolder = new ImmutableHolder(activeMemtable, null);
            this.immutableMemtables.add(recoveredHolder);
            sstableService.submitFlushTask(recoveredHolder, () -> {
                this.immutableMemtables.remove(recoveredHolder);
                for (Path oldWal : oldWalFiles) {
                    try {
                        Files.deleteIfExists(oldWal);
                    } catch (IOException e) {
                        System.err.println("Failed to clean old WAL after recovery: " + oldWal);
                    }
                }
            });
            this.activeMemtable = new Memtable();
        } else {
            for (Path oldWal : oldWalFiles) {
                Files.deleteIfExists(oldWal);
            }
        }
        this.activeWal = new WAL(WAL.generateWALPath(rootPath));
    }

    /**
     * Stores a key-value pair in the memtable and WAL.
     *
     * Writes the entry to both the active WAL for durability and the active memtable
     * for fast access. If the memtable exceeds the maximum size, triggers a rotation
     * and flush operation to persist data to disk.
     *
     * @param key the key to store
     * @param value the value associated with the key
     * @throws IOException if an I/O error occurs during WAL writing
     */
    public void put(String key, String value) throws IOException {
        rwLock.readLock().lock();
        try {
            activeWal.writeEntry(key, value);
            this.activeMemtable.put(key, value);
        } finally {
            rwLock.readLock().unlock();
        }

        if (activeMemtable.getSize() > Constants.MAXSIZE_MEMTABLE) {
            if (isRotating.compareAndSet(false, true)) {
                try {
                    rotateAndFlush();
                } finally {
                    isRotating.set(false);
                }
            }
        }
    }

    private void rotateAndFlush() throws IOException {
        rwLock.writeLock().lock();
        ImmutableHolder holder;
        try {
            holder = new ImmutableHolder(activeMemtable, activeWal);
            immutableMemtables.add(holder);
            this.activeMemtable = new Memtable();
            this.activeWal = new WAL(manifest.getRootPath().resolve(
                    Constants.WAL_PREFIX + System.nanoTime() + Constants.WAL_FILE_EXTENSION));
        } finally {
            rwLock.writeLock().unlock();
        }
        sstableService.submitFlushTask(holder, ()->{
//      try {
//        if (holder.getWal() != null) {
//          holder.getWal().delete();
//        }
//      } catch (IOException e) {
//        System.err.println("Warning: Failed to delete WAL file after flush: " + e.getMessage());
//      } finally {
            immutableMemtables.remove(holder);
        });
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * Searches for the key in the following order:
     * 1. Active memtable (most recent data)
     * 2. Immutable memtables (in reverse chronological order)
     *
     * Returns null if the key is not found in any memtable.
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if not found
     */
    public String get(String key) {
        String val = this.activeMemtable.get(key);
        if (val != null) {
            return val;
        }
        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            ImmutableHolder holder = immutableMemtables.get(i);
            val =  holder.getMemtable().get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }

//  private void cleanOldWals() throws IOException {
//    try (Stream<Path> files = Files.list(rootPath)) {
//      files.filter(path -> path.getFileName().toString().startsWith(Constants.WAL_PREFIX))
//          .filter(path -> path.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION))
//          .forEach(path -> {
//            try {
//              Files.deleteIfExists(path);
//            } catch (IOException e) {
//              System.err.println("Failed to delete old WAL: " + path);
//            }
//          });
//    }

    /**
     * Closes the memtable service and ensures all data is persisted.
     *
     * If there is unflushed data in the active memtable, it schedules a final flush
     * operation and waits for completion. Otherwise, simply closes the active WAL.
     * This method ensures no data loss during shutdown.
     *
     * @throws IOException if an I/O error occurs during shutdown
     */
    public void close() throws IOException {
        if(activeMemtable.getSize() > 0) {
            CountDownLatch latch = new CountDownLatch(1);
            ImmutableHolder finalHolder = new ImmutableHolder(activeMemtable, activeWal);
            sstableService.submitFlushTask(finalHolder, ()->{
                rwLock.writeLock().lock();
                try {
                    this.immutableMemtables.clear();
                } finally {
                    rwLock.writeLock().unlock();
                    latch.countDown();
                }
            });
            try{
                latch.await();
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                System.err.println("Shutdown interrupted while flushing active memtable.");
            }
        } else {
            activeWal.close();
        }
    }
}
