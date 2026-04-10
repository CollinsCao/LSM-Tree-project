package com.util;

public class Constants {

    public static final String DEFAULT_DATA_DIR = "./data";
    public static final String CURRENT_FILENAME = "CURRENT";
    public static final String MANIFEST_PREFIX = "MANIFEST-";
    public static final String MANIFEST_FILENAME = "MANIFEST";
    public static final String WAL_PREFIX = "wal-";
    public static final String WAL_FILE_EXTENSION = ".log";
    public static final String SSTABLE_PREFIX = "sstable-";
    public static final String SSTABLE_FILE_EXTENSION = ".sst";

    public static final String TOMBSTONE = "<TOMBSTONE>";

    public static final int MAX_LEVEL = 7;
    public static final int MAXSIZE_MEMTABLE = 1024;
    public static final int ENTRY_NODE_OVERHEAD = 32;
    public static final long EXPECTED_INSERTIONS = 10_000;
    public static final double FALSE_POSITIVE_PROBABILITY = 0.03;

    private Constants() {}
}
