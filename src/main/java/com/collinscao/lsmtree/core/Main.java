package com.collinscao.lsmtree.core;

import java.util.Scanner;

/**
 * Command-line interface for the LSM Tree database.
 *
 * This class provides a simple interactive shell for performing basic database operations
 * including storing, retrieving, and deleting key-value pairs. It supports the following commands:
 * * put <key> <value> - Store a key-value pair
 * * get <key> - Retrieve the value for a key
 * * del <key> - Delete a key-value pair
 * * exit - Exit the application
 */
public class Main {

    /**
     * Main entry point for the LSM Tree database command-line interface.
     *
     * Initializes the database, starts an interactive command loop, and processes user commands
     * for database operations. The application supports put, get, delete, and exit operations
     * through a simple text-based interface.
     *
     * @param args command-line arguments (currently unused)
     */
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