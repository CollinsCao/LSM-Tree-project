package com.collinscao.lsmtree.core;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.util.Constants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Write-Ahead Log (WAL) functionality within the LSM storage engine,
 * focusing on recovery, persistence, and log rotation.
 */
class WALIntegrationTest {

    @TempDir
    Path tempDir;

    private DB db;

    @BeforeEach
    void setUp() throws IOException {
        db = new DB(tempDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    /**
     * Counts the number of WAL files currently present in the data directory.
     * This is used to track log lifecycle transitions and cleanup events.
     */
    private long countWalFiles() throws IOException {
        try (Stream<Path> files = Files.list(tempDir)) {
            return files.filter(p -> p.getFileName().toString().startsWith(Constants.WAL_PREFIX)
                    && p.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION)).count();
        }
    }

    @Test
    @DisplayName("Verify data recovery from multiple WAL files after a crash")
    void testCrashRecoveryWithMultipleWals() throws IOException {
        // 1. Write the first batch and verify WAL creation
        db.put("key1", "val1");
        assertTrue(countWalFiles() > 0, "WAL file should be created after a write operation");

        // 2. Simulate a crash by nullifying the reference without a graceful shutdown
        db = null;

        // 3. Restart the database instance
        DB newDb = new DB(tempDir.toString());

        // 4. Verify data is successfully replayed from the WAL
        assertEquals("val1", newDb.get("key1"), "Data should be recovered from WAL after restart");

        // 5. Perform subsequent write and simulate another restart to test multi-WAL replay
        newDb.put("key2", "val2");
        newDb = null;

        DB thirdDb = new DB(tempDir.toString());
        assertEquals("val1", thirdDb.get("key1"));
        assertEquals("val2", thirdDb.get("key2"));

        thirdDb.close();
    }

    @Test
    @DisplayName("Verify that obsolete WAL files are cleaned up after successful recovery/flush")
    void testWalCleanupAfterRecovery() throws IOException, InterruptedException {
        // 1. Initialize data and ensure an active WAL is generated
        db.put("key_to_clean", "some_value");
        assertEquals(1, countWalFiles(), "Initial DB should have 1 active WAL");

        // 2. Simulate a non-graceful shutdown
        db = null;

        // 3. Restart the database.
        // The engine should trigger synchronous recovery followed by an asynchronous flush and cleanup.
        DB newDb = new DB(tempDir.toString());

        // Verify synchronous data availability
        assertEquals("some_value", newDb.get("key_to_clean"));

        // 4. Poll until the background thread deletes the recovered (obsolete) WAL file.
        // Total count should drop to 1 (the current active WAL created by newDb).
        int maxRetries = 50;
        while (countWalFiles() > 1 && maxRetries > 0) {
            Thread.sleep(100);
            maxRetries--;
        }

        assertEquals(1, countWalFiles(), "Obsolete WAL files must be deleted after background recovery flush finishes.");

        newDb.close();
    }

    @Test
    @DisplayName("Verify WAL durability: data must be retrievable immediately after restart")
    void testDurability() throws IOException {
        db.put("durability_key", "standard_val");

        // Bypass graceful closure to test durability of logged operations
        db = null;

        DB restartedDb = new DB(tempDir.toString());
        assertEquals("standard_val", restartedDb.get("durability_key"));
        restartedDb.close();
    }
}
