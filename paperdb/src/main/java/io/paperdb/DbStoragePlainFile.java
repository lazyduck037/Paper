package io.paperdb;

import android.content.Context;
import android.util.Log;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import static io.paperdb.Paper.TAG;

class DbStoragePlainFile {

    private final DbManager dbManager;
    private final HashMap<Class, com.esotericsoftware.kryo.kryo5.Serializer> mCustomSerializers;
    private final KeyLocker keyLocker = new KeyLocker(); // To sync key-dependent operations by key
    private final PaperDbKryo5Factory mPaperDbKryo5Factory;
    private final ReadContentKryo5 readContentKryo5;

    private com.esotericsoftware.kryo.kryo5.Kryo getKryo() {
        return mKryo.get();
    }

    private final ThreadLocal<com.esotericsoftware.kryo.kryo5.Kryo> mKryo;


    DbStoragePlainFile(Context context, String dbName,
                       HashMap<Class, com.esotericsoftware.kryo.kryo5.Serializer> serializers) {
        mCustomSerializers = serializers;
        mPaperDbKryo5Factory = new PaperDbKryo5Factory(serializers);
        mKryo = new ThreadLocalKryo(mPaperDbKryo5Factory);
        String mDbPath = context.getFilesDir() + File.separator + dbName;
        readContentKryo5 = new ReadContentKryo5(mKryo);
        dbManager = new DbManager(mDbPath);
    }

    DbStoragePlainFile(String dbFilesDir, String dbName,
                       HashMap<Class, com.esotericsoftware.kryo.kryo5.Serializer> serializers) {
        mCustomSerializers = serializers;
        String dbPath = dbFilesDir + File.separator + dbName;
        mPaperDbKryo5Factory = new PaperDbKryo5Factory(serializers);
        mKryo = new ThreadLocalKryo(mPaperDbKryo5Factory);
        readContentKryo5 = new ReadContentKryo5(mKryo);
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
            return readTableFile(key, originalFile);
        } finally {
            keyLocker.release(key);
        }
    }

    boolean exists(String key) {
        try {
            keyLocker.acquire(key);
            return dbManager.existsInternal(key);
        } finally {
            keyLocker.release(key);
        }
    }

    long lastModified(String key) {
        try {
            keyLocker.acquire(key);
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

    void deleteIfExists(String key) {
        try {
            keyLocker.acquire(key);
            dbManager.deleteTable(key);
        } finally {
            keyLocker.release(key);
        }
    }

    void setLogLevel(int level) {
        com.esotericsoftware.kryo.kryo5.minlog.Log.set(level);
    }

    String getOriginalFilePath(String key) {
        return dbManager.getOriginalFilePath(key);
    }

    String getRootFolderPath() {
        return dbManager.getDbPath();
    }

    private File getOriginalFile(String key) {
        final String tablePath = getOriginalFilePath(key);
        return new File(tablePath);
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
            getKryo().writeObject(kryoOutput, paperTable);
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

    private <E> E readTableFile(String key, File originalFile) {
        try {
            return readContentKryo5.readContent(originalFile);
        } catch (FileNotFoundException | com.esotericsoftware.kryo.kryo5.KryoException | ClassCastException e) {
            Throwable exception = e;
            // Give one more chance, read data in paper 1.x compatibility mode
            if (e instanceof com.esotericsoftware.kryo.kryo5.KryoException) {
                try {
                    return readContentKryo5.readContentRetry(originalFile, mPaperDbKryo5Factory.createKryoInstance(true));
                } catch (FileNotFoundException | com.esotericsoftware.kryo.kryo5.KryoException | ClassCastException compatibleReadException) {
                    exception = compatibleReadException;
                }
            }
            String errorMessage = "Couldn't read/deserialize file "
                    + originalFile + " for table " + key;
            throw new PaperDbException(errorMessage, exception);
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

