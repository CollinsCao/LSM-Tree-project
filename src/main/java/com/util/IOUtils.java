package com.util;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;

public class IOUtils {

  public static String deserializeValue(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}