package io.paperdb;

import android.content.Context;
import android.util.Log;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import static io.paperdb.Paper.TAG;
import com.esotericsoftware.kryo.kryo5.Serializer;
import io.paperdb.kryo4.PaperDbKryo4Factory;
import io.paperdb.kryo4.ReadContentKryo4;
import io.paperdb.kryo4.ThreadLocalKryo4;
import io.paperdb.kryo5.PaperDbKryo5Factory;
import io.paperdb.kryo5.ReadContentKryo5;
import io.paperdb.kryo5.ThreadLocalKryo5;

class DbStoragePlainFile {
    private final boolean mIsMigration;
    private final DbManager dbManager;
    private DbManagerV4 dbManagerV4;
    private final KeyLocker keyLocker = new KeyLocker(); // To sync key-dependent operations by key

    private final Operation operation;

    private final boolean mForceUse4;

    DbStoragePlainFile(Context context, String dbName,
                       HashMap<Class, Serializer> serializersV5,
                       HashMap<Class, com.esotericsoftware.kryo.Serializer> serializersV4,
                       boolean isMigration,
                       boolean forceUse4
                       ) {
        mForceUse4 = forceUse4;
        if (!forceUse4) mIsMigration = isMigration; else mIsMigration = false;

        ReadContentKryo5 readContentKryo5 = new ReadContentKryo5(new ThreadLocalKryo5(new PaperDbKryo5Factory(serializersV5)));
        ReadContentKryo4 readContentKryo4 = new ReadContentKryo4( new ThreadLocalKryo4(new PaperDbKryo4Factory(serializersV4)));
        operation = new Operation(readContentKryo5, readContentKryo4, isMigration);

        String dbPath = context.getFilesDir() + File.separator + dbName;
        String oldDbPath = null;
        if (isMigration){
            oldDbPath = dbPath;
            dbPath = dbPath + "New";
            dbManagerV4 =  new DbManagerV4(oldDbPath);
        }
        dbManager = new DbManager(dbPath);
    }

    DbStoragePlainFile(String dbFilesDir, String dbName,
                       HashMap<Class, Serializer> serializersV5,
                       HashMap<Class, com.esotericsoftware.kryo.Serializer> serializersV4,
                       boolean isMigration,
                       boolean forceUse4
                       ) {
        mForceUse4 = forceUse4;
        if (!forceUse4) mIsMigration = isMigration; else mIsMigration = false;

        ReadContentKryo5 readContentKryo5 = new ReadContentKryo5(new ThreadLocalKryo5(new PaperDbKryo5Factory(serializersV5)));
        ReadContentKryo4 readContentKryo4 = new ReadContentKryo4(new ThreadLocalKryo4(new PaperDbKryo4Factory(serializersV4)));
        operation = new Operation(readContentKryo5, readContentKryo4, isMigration);

        String dbPath = dbFilesDir + File.separator + dbName;
        String oldDbPath = null;
        if (isMigration){
            oldDbPath = dbPath;
            dbPath = dbPath + "New";
            dbManagerV4 =  new DbManagerV4(oldDbPath);
        }
        dbManager = new DbManager(dbPath);
    }

    void destroy() {
        // Acquire global lock to make sure per-key operations (read, write etc) completed
        // and block future per-key operations until destroy is completed
        try {
            keyLocker.acquireGlobal();
            if (!dbManager.destroy()) {
                Log.e(TAG, "Couldn't delete Paper dir " + dbManager.getDbPath());
            }

            if (mIsMigration) {
                if (!dbManagerV4.destroy()) {
                    Log.e(TAG, "Couldn't delete Paper dir " + dbManagerV4.getDbPath());
                }
            }

        } finally {
            keyLocker.releaseGlobal();
        }
    }

    <E> void insert(String key, E value) {
        try {
            keyLocker.acquire(key);
            dbManager.assertInit();

            final File originalFile = dbManager.getOriginalFile(key);
            final File backupFile = dbManager.makeBackupFile(originalFile);
            // Rename the current file so it may be used as a backup during the next readFile
            if(!dbManager.createBackWrite(originalFile, backupFile)){
                throw new PaperDbException("Couldn't rename file " + originalFile
                        + " to backup file " + backupFile);
            }

            final PaperTable<E> paperTable = new PaperTable<>(value);
            writeTableFile(key, paperTable, originalFile, backupFile);
        } finally {
            keyLocker.release(key);
        }
    }

    <E> E select(String key) {
        try {
            keyLocker.acquire(key);
            dbManager.assertInit();
            final File originalFile = dbManager.getOriginalFile(key);
            final File backupFile = dbManager.makeBackupFile(originalFile);
            if(!dbManager.createBackUpBeforeSelect(originalFile, backupFile, key)){
                return null;
            }
            if (mIsMigration){
                if(dbManager.existsInternal(key)){
                    return operation.readTableFile(key, originalFile);
                }

                //Read Old File.
                dbManagerV4.assertInit();
                final File originalFileV4 = dbManagerV4.getOriginalFile(key);
                final File backupFileV4 = dbManagerV4.makeBackupFile(originalFileV4);
                if(!dbManagerV4.createBackUpBeforeSelect(originalFileV4, backupFileV4, key)){
                    return null;
                }
                return operation.readTableFileV4(key, originalFile);
            }else {
                return operation.readTableFile(key, originalFile);
            }
        } finally {
            keyLocker.release(key);
        }
    }

    boolean exists(String key) {
        try {
            keyLocker.acquire(key);
            if (mIsMigration){
                boolean res = dbManager.existsInternal(key);
                if (!res){
                    return dbManagerV4.existsInternal(key);
                }
                return true;
            }
            return dbManager.existsInternal(key);
        } finally {
            keyLocker.release(key);
        }
    }

    long lastModified(String key) {
        try {
            keyLocker.acquire(key);
            if (mIsMigration){
                if(dbManager.existsInternal(key)){
                    return dbManager.lastModified(key);
                }
                return dbManagerV4.lastModified(key);
            }
            return dbManager.lastModified(key);
        } finally {
            keyLocker.release(key);
        }
    }

    List<String> getAllKeys() {
        try {
            // Acquire global lock to make sure per-key operations (delete etc) completed
            // and block future per-key operations until reading for all keys is completed
            keyLocker.acquireGlobal();
            return dbManager.getAllKeys();
        } finally {
            keyLocker.releaseGlobal();
        }
    }

    List<String> getTotalKey() {
        try {
            // Acquire global lock to make sure per-key operations (delete etc) completed
            // and block future per-key operations until reading for all keys is completed
            keyLocker.acquireGlobal();
            List<String> currentDb =  dbManager.getAllKeys();
            List<String> oldDb;
            if (mIsMigration){
                oldDb = dbManagerV4.getAllKeys();
            }else {
                oldDb = new ArrayList<>();
            }
            HashSet<String> res = new HashSet<>(currentDb);
            res.addAll(oldDb);
            return new ArrayList<String>(res);
        } finally {
            keyLocker.releaseGlobal();
        }
    }

    List<String> getAllKeysV4() {
        try {
            // Acquire global lock to make sure per-key operations (delete etc) completed
            // and block future per-key operations until reading for all keys is completed
            keyLocker.acquireGlobal();
            if (!mIsMigration) return new ArrayList<>();
            return dbManagerV4.getAllKeys();
        } finally {
            keyLocker.releaseGlobal();
        }
    }

    void deleteIfExists(String key) {
        try {
            keyLocker.acquire(key);
            if(mIsMigration){
                dbManagerV4.deleteTable(key);
            }
            dbManager.deleteTable(key);
        } finally {
            keyLocker.release(key);
        }
    }

    void setLogLevel(int level) {
        com.esotericsoftware.kryo.kryo5.minlog.Log.set(level);
        if (mIsMigration){
            com.esotericsoftware.minlog.Log.set(level);
        }
    }

    String getOriginalFilePath(String key) {
        return dbManager.getOriginalFilePath(key);
    }

    String getOriginalFilePathV4(String key) {
        return dbManagerV4.getOriginalFilePath(key);
    }

    String getRootFolderPath() {
        return dbManager.getDbPath();
    }

    String getRootFolderPathV4() {
        return dbManagerV4.getDbPath();
    }

    /**
     * Attempt to write the file, delete the backup and return true as atomically as
     * possible.  If any exception occurs, delete the new file; next time we will restore
     * from the backup.
     *
     * @param key          table key
     * @param paperTable   table instance
     * @param originalFile file to write new data
     * @param backupFile   backup file to be used if write is failed
     */
    private <E> void writeTableFile(String key, PaperTable<E> paperTable,
                                    File originalFile, File backupFile) {
        com.esotericsoftware.kryo.kryo5.io.Output kryoOutput = null;
        try {
            FileOutputStream fileStream = new FileOutputStream(originalFile);
            kryoOutput = new com.esotericsoftware.kryo.kryo5.io.Output(fileStream);
            operation.getKryo().writeObject(kryoOutput, paperTable);
            kryoOutput.flush();
            fileStream.flush();
            sync(fileStream);
            kryoOutput.close(); //also close file stream
            kryoOutput = null;

            // Writing was successful, delete the backup file if there is one.
            //noinspection ResultOfMethodCallIgnored
            backupFile.delete();
        } catch (IOException | com.esotericsoftware.kryo.kryo5.KryoException e) {
            // Clean up an unsuccessfully written file
            if (originalFile.exists()) {
                if (!originalFile.delete()) {
                    throw new PaperDbException("Couldn't clean up partially-written file "
                            + originalFile, e);
                }
            }
            throw new PaperDbException("Couldn't save table: " + key + ". " +
                    "Backed up table will be used on next read attempt", e);
        } finally {
            if (kryoOutput != null) {
                kryoOutput.close();  // closing opened kryo output with initial file stream.
            }
        }
    }



    /**
     * Perform an fsync on the given FileOutputStream.  The stream at this
     * point must be flushed but not yet closed.
     */
    private static void sync(FileOutputStream stream) {
        //noinspection EmptyCatchBlock
        try {
            if (stream != null) {
                stream.getFD().sync();
            }
        } catch (IOException e) {
        }
    }
}

