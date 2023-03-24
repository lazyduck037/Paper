package io.paperdb;

import java.io.File;

/**
 * @author lazyduck037
 */
public class Utils {
   public static boolean deleteDirectory(String dbPath) {
      // Acquire global lock to make sure per-key operations (read, write etc) completed
      // and block future per-key operations until destroy is completed
      File directory = new File(dbPath);
      if (directory.exists()) {
         File[] files = directory.listFiles();
         if (null != files) {
            for (File file : files) {
               if (file.isDirectory()) {
                  deleteDirectory(file.toString());
               } else {
                  //noinspection ResultOfMethodCallIgnored
                  file.delete();
               }
            }
         }
      }
      return directory.delete();
   }
}
