package com.util;

import com.collinscao.lsmtree.memtable.Memtable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Write-Ahead Log (WAL) implementation for durability in the LSM Tree database.
 *
 * This class provides persistent logging of database operations to ensure data
 * durability and enable crash recovery. WAL entries are written synchronously
 * and can be replayed to reconstruct the in-memory state after a failure.
 */
public class WAL implements AutoCloseable{
    private Path filePath;
    private DataOutputStream out;

    /**
     * Creates a new WAL instance for the specified file path.
     *
     * Opens the file in append mode, allowing continued writing to an existing
     * WAL file or creation of a new one.
     *
     * @param filePath the path to the WAL file
     * @throws IOException if an I/O error occurs while opening the file
     */
    public WAL(Path filePath) throws IOException {
        this.filePath = filePath;
        this.out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)));
    }

    /**
     * Writes a key-value entry to the WAL.
     *
     * Serializes the key-value pair with length prefixes and appends it to the WAL file.
     * The format is: KeyLen(4) + Key + ValueLen(4) + Value.
     *
     * @param key the key to write
     * @param value the value to write
     * @throws IOException if an I/O error occurs during writing
     */
    public synchronized void writeEntry(String key, String value) throws IOException {
        // append : KeyLen(4) + Key + ValLen(4) + Value
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.writeInt(valueBytes.length);
        out.write(valueBytes);

        out.flush();
    }

    /**
     * Deletes the WAL file from disk.
     *
     * Closes the WAL first, then attempts to delete the file. Safe to call
     * even if the file doesn't exist.
     *
     * @throws IOException if an I/O error occurs during closing or deletion
     */
    public void delete() throws IOException {
        this.close();
        Files.deleteIfExists(filePath);
    }

    /**
     * Closes the WAL and releases associated resources.
     *
     * Flushes and closes the underlying output stream.
     *
     * @throws IOException if an I/O error occurs during closing
     */
    @Override
    public void close() throws IOException {
        out.close();
    }

    /**
     * Recovers all WAL files in the specified directory into a memtable.
     *
     * Scans the directory for WAL files, sorts them by filename, and replays
     * each file's entries into the provided memtable for crash recovery.
     *
     * @param rootPath the directory containing WAL files
     * @param memtable the memtable to populate with recovered data
     * @throws IOException if an I/O error occurs during recovery
     */
    public static void recoverAll(Path rootPath, Memtable memtable) throws IOException {
        try(var stream = Files.list(rootPath)){
            List<Path> walFiles = stream
                    .filter(path -> path.getFileName().toString().startsWith(Constants.WAL_PREFIX))
                    .filter(path -> path.getFileName().toString().endsWith(Constants.WAL_FILE_EXTENSION))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
            for (Path walFile : walFiles){
                recoverMemtableFromWal(walFile, memtable);
            }
        }
    }

    /**
     * Recovers entries from a single WAL file into a memtable.
     *
     * Reads all entries from the specified WAL file and applies them to the memtable.
     * Handles truncated files gracefully by logging a warning and stopping recovery.
     *
     * @param walPath the path to the WAL file to recover
     * @param memtable the memtable to populate with recovered data
     * @throws IOException if an I/O error occurs during recovery
     */
    public static void recoverMemtableFromWal(Path walPath, Memtable memtable) throws IOException {
        if (!Files.exists(walPath)) {return;}

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(walPath)))) {
            while (in.available() > 0) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                int valueLen = in.readInt();
                byte[] valueBytes = new byte[valueLen];
                in.readFully(valueBytes);

                memtable.put(new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8),
                        new String(valueBytes, java.nio.charset.StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            System.err.println("Warning: WAL file ended unexpectedly (truncated). Recovered data up to the break.");
        }
    }

    /**
     * Generates a unique file path for a new WAL file.
     *
     * Creates a path using the WAL prefix, current nanosecond timestamp,
     * and file extension to ensure uniqueness.
     *
     * @param rootPath the root directory for WAL files
     * @return a unique path for the new WAL file
     */
    public static Path generateWALPath(Path rootPath) {
        return rootPath.resolve(Constants.WAL_PREFIX + System.nanoTime() + Constants.WAL_FILE_EXTENSION);
    }
}
