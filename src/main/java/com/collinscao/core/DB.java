package com.collinscao.core;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.memtable.MemtableService;
import com.collinscao.lsmtree.sstable.SSTableService;
import java.io.IOException;
import com.util.Constants;

public class DB implements AutoCloseable {
    private final Manifest manifest;
    private final MemtableService memtableService;
    private final SSTableService sstableService;

    public DB() throws IOException {
        this(Constants.DEFAULT_DATA_DIR);
    }

    public DB(String dataDir) throws IOException {
        manifest = new Manifest(dataDir);
        sstableService = new SSTableService(manifest);
        memtableService = new MemtableService(manifest, sstableService);
    }

    public void put(String key, String value) throws IOException {
        memtableService.put(key, value);
    }

    public String get(String key) {
        String value = memtableService.get(key);
        if (value == null) {
            value = sstableService.get(key);
        }
        return (value == null || value.equals(Constants.TOMBSTONE)) ? null : value;
    }

    public void remove(String key) throws IOException {
        memtableService.put(key, Constants.TOMBSTONE);
    }

    @Override
    public void close() throws IOException {
        memtableService.close();
        sstableService.close();
    }
}