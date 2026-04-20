package com.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for I/O operations in the LSM Tree database.
 *
 * This class provides static methods for reading and deserializing data
 * from files and byte arrays, primarily used for SSTable and WAL operations.
 */
public class IOUtils {

  /**
   * Reads a string from a RandomAccessFile.
   *
   * Reads an integer length prefix followed by the corresponding number of bytes,
   * then converts the bytes to a UTF-8 string.
   *
   * @param raf the RandomAccessFile to read from
   * @return the decoded string
   * @throws IOException if an I/O error occurs during reading
   */
  public static String readNextString(RandomAccessFile raf) throws IOException {
    int len = raf.readInt();
    byte[] buf = new byte[len];
    raf.readFully(buf);
    return new String(buf, StandardCharsets.UTF_8);
  }

  /**
   * Deserializes a byte array into a string.
   *
   * Converts the provided byte array to a UTF-8 encoded string.
   *
   * @param bytes the byte array to deserialize
   * @return the decoded string
   */
  public static String deserializeValue(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}