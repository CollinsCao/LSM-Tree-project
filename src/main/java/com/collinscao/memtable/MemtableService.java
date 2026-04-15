package com.collinscao.memtable;

import com.andrea.lsm.manifest.Manifest;
import com.andrea.lsm.sstable.SSTableService;
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

public class MemtableService {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicBoolean isRotating = new AtomicBoolean(false);

    private final Manifest manifest;
    private final SSTableService sstableService;
    private volatile Memtable activeMemtable;
    private volatile WAL activeWal;
    private final Path rootPath;
    private volatile CopyOnWriteArrayList<ImmutableHolder> immutableMemtables;

    public static class ImmutableHolder {

        private final Memtable memtable;
        private final WAL wal;

        public ImmutableHolder(Memtable memtable, WAL wal) {
            this.memtable = memtable;
            this.wal = wal;
        }

        public Memtable getMemtable() {
            return memtable;
        }

        public WAL getWal() {
            return wal;
        }
    }

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
