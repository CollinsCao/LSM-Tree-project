package com.util;

/**
 * Constants used throughout the LSM Tree database implementation.
 *
 * This class contains configuration values, file naming patterns, and other
 * constants that control the behavior of various database components including
 * file management, memory thresholds, and data structure parameters.
 */
public class Constants {

    // File and directory constants
    /** Default directory for storing database files. */
    public static final String DEFAULT_DATA_DIR = "./data";
    /** Name of the CURRENT file that points to the active manifest. */
    public static final String CURRENT_FILENAME = "CURRENT";
    /** Prefix for manifest files. */
    public static final String MANIFEST_PREFIX = "MANIFEST-";
    /** Base name for manifest files. */
    public static final String MANIFEST_FILENAME = "MANIFEST";

    // WAL (Write-Ahead Log) constants
    /** Prefix for WAL files. */
    public static final String WAL_PREFIX = "wal-";
    /** File extension for WAL files. */
    public static final String WAL_FILE_EXTENSION = ".log";

    // SSTable constants
    /** Prefix for SSTable files. */
    public static final String SSTABLE_PREFIX = "sstable-";
    /** File extension for SSTable files. */
    public static final String SSTABLE_FILE_EXTENSION = ".sst";

    // Data constants
    /** Special marker value indicating a deleted key. */
    public static final String TOMBSTONE = "<TOMBSTONE>";

    // System configuration constants
    /** Maximum number of levels in the LSM Tree. */
    public static final int MAX_LEVEL = 7;
    /** Maximum size of memtable before triggering flush (in entries). */
    public static final int MAXSIZE_MEMTABLE = 1024;
    /** Overhead per entry in data structures (in bytes). */
    public static final int ENTRY_NODE_OVERHEAD = 32;

    // Bloom filter configuration
    /** Expected number of insertions for Bloom filter sizing. */
    public static final long EXPECTED_INSERTIONS = 10_000;
    /** Target false positive probability for Bloom filters. */
    public static final double FALSE_POSITIVE_PROBABILITY = 0.03;

    /**
     * Private constructor to prevent instantiation.
     *
     * This class contains only static constants and should not be instantiated.
     */
    private Constants() {}
}
