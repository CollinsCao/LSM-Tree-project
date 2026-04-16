package com.collinscao.lsmtree.memtable;

import com.collinscao.lsmtree.manifest.Manifest;
import com.collinscao.lsmtree.memtable.MemtableService;
import com.collinscao.lsmtree.sstable.SSTable;
import com.collinscao.lsmtree.sstable.SSTableService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.Constants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MemtableService}, validating the lifecycle of
 * in-memory data, including automatic and manual flushing to persistent storage.
 */
class MemtableServiceTest {

    @TempDir
    Path tempDir; // JUnit 5 manages this lifecycle-bound temporary directory

    private MemtableService memtableService;
    private Manifest manifest;
    private SSTableService sstableService;

    @BeforeEach
    void setUp() throws IOException {
        // 1. Initialize Manifest within the temporary directory for metadata tracking
        manifest = new Manifest(tempDir.toString());

        // 2. Initialize the SSTable service with the manifest
        sstableService = new SSTableService(manifest);

        // 3. Initialize the memtable service with required dependencies
        memtableService = new MemtableService(manifest, sstableService);
    }

    @AfterEach
    void tearDown() {
        // Shut down the background thread pool to prevent resource leaks and cross-test interference
        if (sstableService != null) {
            sstableService.stop();
        }
    }

    @Test
    void testPutAndGetInMemory() throws IOException {
        // Validate basic volatile storage operations (Read-Your-Writes consistency)
        memtableService.put("key1", "value1");
        String val = memtableService.get("key1");

        assertEquals("value1", val, "Data should be retrievable from the active memtable");

        // Ensure no premature persistence has occurred
        assertEquals(0, manifest.getSSTable(0).size(),
                "Data should not be flushed to disk before reaching the size threshold");
    }

    @Test
    void testFlushOnThreshold() throws IOException, InterruptedException {
        // Objective: Trigger an automatic flush by exceeding the memtable size limit
        int threshold = Constants.MAXSIZE_MEMTABLE;

        // Generate a payload that exceeds the threshold
        String bigValue = "x".repeat(threshold + 1);

        // 1. Write the large payload to trigger a background flush
        memtableService.put("bigKey", bigValue);

        // 2. Poll the manifest until the background flush task completes
        int maxRetries = 50;
        while (manifest.getSSTable(0).isEmpty() && maxRetries > 0) {
            Thread.sleep(100); // Wait 100ms per iteration, up to 5 seconds total
            maxRetries--;
        }

        // 3. Verify that the Manifest has registered the new L0 SSTable
        List<SSTable> tables = manifest.getSSTable(0);
        assertEquals(1, tables.size(), "Automatic flush should occur when the size threshold is breached");

        // 4. Verify memtable reset
        assertNull(memtableService.get("bigKey"), "The active memtable should be cleared post-flush");

        // 5. Validate file placement
        SSTable generatedSSTable = tables.get(0);
        String filePath = String.valueOf(generatedSSTable.getFilePath());
        assertTrue(filePath.startsWith(tempDir.toString()),
                "The SSTable file must be stored within the designated temporary directory");
    }

    @Test
    void testCloseTriggersFlush() throws IOException {
        // 1. Perform a small write that does not trigger the threshold
        memtableService.put("k_close", "v_close");

        // Verify data remains exclusively in memory
        assertEquals(0, manifest.getSSTable(0).size());

        // 2. Perform a graceful shutdown
        // The close() method is synchronous and blocks until all pending flushes complete
        memtableService.close();

        // 3. Validation
        // Assert immediately as the synchronous close ensures data persistence
        List<SSTable> tables = manifest.getSSTable(0);
        assertEquals(1, tables.size(), "Service closure must trigger a mandatory flush to prevent data loss");

        // Verify data integrity in the resulting SSTable
        SSTable sst = tables.get(0);
        assertEquals("v_close", sst.get("k_close"), "Flushed data must be readable from disk");
    }
}
