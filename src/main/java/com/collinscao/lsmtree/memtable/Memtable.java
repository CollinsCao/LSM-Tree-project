package com.collinscao.memtable;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import com.util.Constants;

/**
 * An in-memory storage component for the LSM Tree architecture.
 * <p>
 * Data is stored in a TreeMap (an automatically sorted data structure) in memory,
 * and will be flushed to SSTables on disk when its size reaches 4kb.
 * <p>
 */
public class Memtable {

    /**
     * Underlying storage in memory using a Red-Black tree to keep keys sorted.
     */
    private ConcurrentSkipListMap<String, String> table;

    /**
     * Tracks the estimated size of the data in bytes when flushed to SSTables.
     */
    private AtomicLong approximateSize;

    /**
     * Create an empty Memtable for buffering incoming writes.
     */
    public Memtable() {
        this.table = new ConcurrentSkipListMap<>();
        this.approximateSize = new AtomicLong(0);
    }

    /**
     * Inserts or updates a key-value pair in the Memtable.
     * @param key The key to insert (must not be null).
     * @param value The value associated with the key.
     */
    public void put(String key, String value) {
        String oldValue = table.put(key, value);
        long addedSize;
        if (oldValue == null) {
            addedSize = (long)key.length() + value.length() + Constants.ENTRY_NODE_OVERHEAD;
        } else {
            addedSize = (long)value.length() - oldValue.length();
        }
        approximateSize.getAndAdd(addedSize);
    }

    /**
     * Retrieve the value associated with the given key.
     * <p>
     * This method returns the raw value stored in Memtable, which may
     * include the {@link Constants#TOMBSTONE} marker. The caller
     * (DB layer) is responsible for interpreting this marker as a logical
     * delete-operation.
     * <p>
     * @param key The key whose associated value is to be returned.
     * @return The value associated with the key, or null if key does not exist.
     */
    public String get(String key) {
        String value = table.get(key);
        return value;
    }

    /**
     * @return The estimated size in bytes when flushed to SSTables.
     */
    public long getSize() {
        return approximateSize.get();
    }

    /**
     * Clears all data from Memtable and reset the size counter to zero.
     * <p>
     * This method is typically called after successfully flushing data from
     * Memtable to SSTables (disk).
     */
    public void clear() {
        table.clear();
        approximateSize = new AtomicLong(0);
    }

    /**
     * Returns an iterator over the entries in this Memtable.
     * <p>
     * The iterator traverses the entries in ascending key order. This is primarily
     * used during the flush process to write data sequentially to disk.
     * <p>
     * @return An iterator over the map entries.
     */
    public Iterator<Entry<String, String>> iterator() {
        return table.entrySet().iterator();
    }
}

