package com.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class IOUtils {

  public static String readNextString(RandomAccessFile raf) throws IOException {
    int len = raf.readInt();
    byte[] buf = new byte[len];
    raf.readFully(buf);
    return new String(buf, StandardCharsets.UTF_8);
  }

  public static String deserializeValue(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}