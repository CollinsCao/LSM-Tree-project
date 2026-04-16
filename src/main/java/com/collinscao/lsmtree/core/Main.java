package com.collinscao.lsmtree.core;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
      System.out.println("Initializing DB...");

      try (DB db = new DB()) {
        System.out.println("DB is running! Enter commands: put <k> <v>, get <k>, del <k>, exit");

        Scanner scanner = new Scanner(System.in);

        while (true) {
          System.out.println("> ");
          String line = scanner.nextLine();
          String[] parts = line.split(" ");
          String cmd = parts[0].toLowerCase();

          switch (cmd) {
            case "put": {
              String key = parts[1];
              String value = parts[2];
              db.put(key, value);
              System.out.println("Put successfully: Key: " + key + " Value: " + value);
              break;
            }
            case "get": {
              String key = parts[1];
              String result = db.get(key);
              System.out.println((result == null) ? "null" : result);
              break;
            }
            case "del": {
              String key = parts[1];
              db.remove(key);
              System.out.println("Not exist or Delete successfully: Key: " + key);
              break;
            }
            case "exit": {
              return;
            }
            default: {
              System.out.println("Unknown command: " + cmd);
            }
          }
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
      System.out.println("DB closed.");
    }
  }