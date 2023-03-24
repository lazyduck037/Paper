package io.paperdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lazyduck037
 */
class DbManagerV4 {
   private static final String BACKUP_EXTENSION = ".bak";
   private final String mOldDbPath;
   private volatile boolean mIsForceUseV4;

   private volatile boolean mPaperDirIsCreatedV4;

   DbManagerV4(String oldDbName, boolean isForceUseV4){
      mOldDbPath = oldDbName;
      mIsForceUseV4 = isForceUseV4;
   }

   public boolean destroy(){
      boolean res = Utils.deleteDirectory(mOldDbPath);
      mPaperDirIsCreatedV4 = false;
      return res;
   }

   synchronized void assertInit() {
      if (!mPaperDirIsCreatedV4) {
         if (!new File(mOldDbPath).exists()) {
            if (mIsForceUseV4) {
               boolean isReady = new File(mOldDbPath).mkdirs();
               if (!isReady) {
                  throw new RuntimeException("Couldn't create Paper dir: " + mOldDbPath);
               }
            }else {
               throw new RuntimeException("Couldn't create Paper dir: " + mOldDbPath);
            }
         }
         mPaperDirIsCreatedV4 = true;
      }
   }

   boolean createBackWrite(File originalFile, File backupFile){
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

   long lastModified(String key) {
      assertInit();
      final File originalFile = getOriginalFile(key);
      return originalFile.exists() ? originalFile.lastModified() : -1;
   }

   List<String> getAllKeys() {
      // Acquire global lock to make sure per-key operations (delete etc) completed
      // and block future per-key operations until reading for all keys is completed
      assertInit();

      File bookFolder = new File(mOldDbPath);
      String[] names = bookFolder.list((file, s) -> !s.endsWith(BACKUP_EXTENSION));
      if (names != null) {
         //remove extensions
         for (int i = 0; i < names.length; i++) {
            names[i] = names[i].replace(".pt", "");
         }
         return Arrays.asList(names);
      } else {
         return new ArrayList<>();
      }
   }

   public void deleteTable(String key) {
      assertInit();

      final File originalFile = getOriginalFile(key);
      if (!originalFile.exists()) {
         return;
      }

      boolean deleted = originalFile.delete();
      if (!deleted) {
         throw new PaperDbException("Couldn't delete file " + originalFile
                 + " for table " + key);
      }
   }

   String getDbPath(){
      return mOldDbPath;
   }

   File getOriginalFile(String key) {
      final String tablePath = getOriginalFilePath(key);
      return new File(tablePath);
   }

   String getOriginalFilePath(String key) {
      return mOldDbPath + File.separator + key + ".pt";
   }

   File makeBackupFile(File originalFile) {
      return new File(originalFile.getPath() + BACKUP_EXTENSION);
   }

   boolean createBackUpBeforeSelect(File originalFile, File backupFile, String key){
      if (backupFile.exists()) {
         //noinspection ResultOfMethodCallIgnored
         originalFile.delete();
         //noinspection ResultOfMethodCallIgnored
         backupFile.renameTo(originalFile);
      }

      if (!existsInternal(key)) {
         return false;
      }
      return true;
   }

   boolean existsInternal(String key) {
      assertInit();
      final File originalFile = getOriginalFile(key);
      return originalFile.exists();
   }
}
