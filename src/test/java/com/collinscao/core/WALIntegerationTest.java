package com.collinscao.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.util.Constants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Write-Ahead Log (WAL) functionality.
 * Verifies data durability and recovery consistency during simulated crashes.
 */
class WALIntegrationTest {

    @TempDir
    Path tempDir;

    private DB db;

    @BeforeEach
    void setUp() throws IOException {
        // Use a unique temporary directory for each test to ensure isolation.
        // Preference: Passing Path directly to avoid manual string conversion.
        db = new DB(tempDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    /**
     * Verifies that the database can recover uncommitted data from the WAL
     * after an unexpected shutdown (crash).
     */
    @Test
    void testCrashRecovery() throws IOException {
        // Phase 1: Write data without triggering a flush to SSTables.
        db.put("key_crash", "value_crash");

        // Ensure data is immediately accessible from the Memtable.
        assertEquals("value_crash", db.get("key_crash"));

        // Phase 2: Verify WAL persistence.
        // The WAL should be created and contain data immediately after the write operation.
        Path walPath = tempDir.resolve(Constants.WAL_FILENAME);
        assertTrue(Files.exists(walPath), "WAL file should be created on disk.");
        assertTrue(Files.size(walPath) > 0, "WAL file should not be empty after put operations.");

        /*
         * Phase 3: Simulate a System Crash.
         * We discard the DB reference without calling db.close() to simulate a process failure.
         * * Note: In a real-world scenario, we rely on the OS or disk sync to ensure
         * the WAL buffer has been physically written to the storage media.
         */
        db = null;

        // Phase 4: Restart the database system.
        // Initializing a new instance from the same directory should trigger the recovery logic.
        DB newDB = new DB(tempDir.toString());

        /*
         * Phase 5: Verify Recovery Consistency.
         * The recovery process must replay the WAL and restore the state of the Memtable.
         */
        String val = newDB.get("key_crash");
        assertEquals("value_crash", val, "Data should be recovered from WAL after restart.");

        // Clean up the new database instance.
        newDB.close();
    }
}
