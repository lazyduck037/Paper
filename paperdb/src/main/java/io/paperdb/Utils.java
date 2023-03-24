package io.paperdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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

   public static boolean createBackWrite(File originalFile, File backupFile) {
      if (originalFile.exists()) {
         //Rename original to backup
         if (!backupFile.exists()) {
            if (!originalFile.renameTo(backupFile)) {
               return false;
            }
         } else {
            //Backup exist -> original file is broken and must be deleted
            //noinspection ResultOfMethodCallIgnored
            originalFile.delete();
         }
      }
      return true;
   }


   /**
    * Perform an fsync on the given FileOutputStream.  The stream at this
    * point must be flushed but not yet closed.
    */
   public static void sync(FileOutputStream stream) {
      //noinspection EmptyCatchBlock
      try {
         if (stream != null) {
            stream.getFD().sync();
         }
      } catch (IOException e) {
      }
   }
}
