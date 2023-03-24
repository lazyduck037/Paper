package io.paperdb;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lazyduck037
 */
class DbManager {
    private static final String BACKUP_EXTENSION = ".bak";
    private final String mDbPath;
    private volatile boolean mPaperDirIsCreated;
    DbManager(String dbName){
        mDbPath = dbName;
    }

    public boolean destroy(){
        boolean res = Utils.deleteDirectory(mDbPath);
        mPaperDirIsCreated = false;
        return res;
    }

    /**
     * Must be synchronized to avoid race conditions on creating dir from different threads
     */
    synchronized void assertInit() {
        if (!mPaperDirIsCreated) {
            if (!new File(mDbPath).exists()) {
                boolean isReady = new File(mDbPath).mkdirs();
                if (!isReady) {
                    throw new RuntimeException("Couldn't create Paper dir: " + mDbPath);
                }
            }
            mPaperDirIsCreated = true;
        }
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

        File bookFolder = new File(mDbPath);
        String[] names = bookFolder.list(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return !s.endsWith(BACKUP_EXTENSION);
            }
        });
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
        return mDbPath;
    }

    File getOriginalFile(String key) {
        final String tablePath = getOriginalFilePath(key);
        return new File(tablePath);
    }

    String getOriginalFilePath(String key) {
        return mDbPath + File.separator + key + ".pt";
    }

    File makeBackupFile(File originalFile) {
        return new File(originalFile.getPath() + BACKUP_EXTENSION);
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
