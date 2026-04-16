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

public class WAL implements AutoCloseable{
    private Path filePath;
    private DataOutputStream out;

    public WAL(Path filePath) throws IOException {
        this.filePath = filePath;
        this.out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)));
    }

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

    public void delete() throws IOException {
        this.close();
        Files.deleteIfExists(filePath);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

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

    public static Path generateWALPath(Path rootPath) {
        return rootPath.resolve(Constants.WAL_PREFIX + System.nanoTime() + Constants.WAL_FILE_EXTENSION);
    }
}
