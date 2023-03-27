package io.paperdb;

import android.content.Context;
import android.util.Log;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import static io.paperdb.Paper.TAG;
import com.esotericsoftware.kryo.kryo5.Serializer;

import io.paperdb.kryo4.DbManagerV4;
import io.paperdb.kryo4.PaperDbKryo4Factory;
import io.paperdb.kryo4.ReadContentKryo4;
import io.paperdb.kryo4.ThreadLocalKryo4;
import io.paperdb.kryo5.DbManager5;
import io.paperdb.kryo5.PaperDbKryo5Factory;
import io.paperdb.kryo5.ReadContentKryo5;
import io.paperdb.kryo5.ThreadLocalKryo5;

class DbStoragePlainFile {
    private final DbManager5 dbManager;
    private DbManagerV4 dbManagerV4;
    private final KeyLocker keyLocker = new KeyLocker(); // To sync key-dependent operations by key

    private final Operation operation;

    private String mDbName;

    DbStoragePlainFile(Context context, String dbName,
                       HashMap<Class, Serializer> serializersV5,
                       HashMap<Class, com.esotericsoftware.kryo.Serializer> serializersV4,
                       boolean isMigration,
                       boolean forceUse4
                       ) {
        ReadContentKryo5 readContentKryo5 = new ReadContentKryo5(new ThreadLocalKryo5(new PaperDbKryo5Factory(serializersV5)));
        ReadContentKryo4 readContentKryo4 = null;
        mDbName = dbName;
        String dbPath = context.getFilesDir() + File.separator + dbName;
        if (isMigration) {
            dbManagerV4 = new DbManagerV4(dbPath);
            readContentKryo4 = new ReadContentKryo4(new ThreadLocalKryo4(new PaperDbKryo4Factory(serializersV4)));
        }
        operation = new Operation(readContentKryo5, readContentKryo4);
        dbManager = new DbManager5(dbPath);
    }

    DbStoragePlainFile(String dbFilesDir, String dbName,
                       HashMap<Class, Serializer> serializersV5,
                       HashMap<Class, com.esotericsoftware.kryo.Serializer> serializersV4,
                       boolean isMigration,
                       boolean forceUse4
                       ) {
        ReadContentKryo5 readContentKryo5 = new ReadContentKryo5(new ThreadLocalKryo5(new PaperDbKryo5Factory(serializersV5)));


        mDbName = dbName;
        String dbPath = dbFilesDir + File.separator + dbName;
        ReadContentKryo4 readContentKryo4 = null;
        if (isMigration) {
            readContentKryo4 = new ReadContentKryo4(new ThreadLocalKryo4(new PaperDbKryo4Factory(serializersV4)));
            dbManagerV4 = new DbManagerV4(dbPath);
        }
        operation = new Operation(readContentKryo5, readContentKryo4);
        dbManager = new DbManager5(dbPath);
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

    private void destroyForce(){
        if (!dbManagerV4.destroy()) {
            Log.e(TAG, "Couldn't delete Paper dir " + dbManagerV4.getDbPath());
        }
    }

    <E> void insert(String key, E value) {
        try {
            keyLocker.acquire(key);
            insertNew(key, value);
        } finally {
            keyLocker.release(key);
        }
    }

    private <E>void insertNew(String key, E value){
        dbManager.assertInit();
        final File originalFile = dbManager.getOriginalFile(key);
        final File backupFile = dbManager.makeBackupFile(originalFile);
        // Rename the current file so it may be used as a backup during the next readFile
        if (!dbManager.createBackWrite(originalFile, backupFile)) {
            throw new PaperDbException("Couldn't rename file " + originalFile
                    + " to backup file " + backupFile);
        }

        final PaperTable<E> paperTable = new PaperTable<>(value);
        operation.writeTableFile(key, paperTable, originalFile, backupFile);
    }

    private <E>void insertOld(String key, E value){
        dbManagerV4.assertInit();
        final File originalFile = dbManagerV4.getOriginalFile(key);
        final File backupFile = dbManagerV4.makeBackupFile(originalFile);
        // Rename the current file so it may be used as a backup during the next readFile
        if (!dbManagerV4.createBackWrite(originalFile, backupFile)) {
            throw new PaperDbException("Couldn't rename file " + originalFile
                    + " to backup file " + backupFile);
        }

        final PaperTable<E> paperTable = new PaperTable<>(value);
        operation.writeTableFileV4(key, paperTable, originalFile, backupFile);
    }

    <E> E select(String key) {
        try {
            keyLocker.acquire(key);
            return selectNew(key);
        } finally {
            keyLocker.release(key);
        }
    }

    <E> E selectOldVersion(String key) {
        dbManagerV4.assertInit();
        final File originalFileV4 = dbManagerV4.getOriginalFile(key);
        final File backupFileV4 = dbManagerV4.makeBackupFile(originalFileV4);
        if(!dbManagerV4.createBackUpBeforeSelect(originalFileV4, backupFileV4, key)){
            return null;
        }
        return operation.readTableFileV4(key, originalFileV4);
    }

    private <E> E selectNew(String key) {
        dbManager.assertInit();
        final File originalFile = dbManager.getOriginalFile(key);
        final File backupFile = dbManager.makeBackupFile(originalFile);
        if(!dbManager.createBackUpBeforeSelect(originalFile, backupFile, key)){
            return null;
        }
        return operation.readTableFile(key, originalFile);
    }

    boolean exists(String key) {
        try {
            keyLocker.acquire(key);
            return dbManager.existsInternal(key);
        } finally {
            keyLocker.release(key);
        }
    }


    private boolean existOld(String key){
        return dbManagerV4.existsInternal(key);
    }

    long lastModified(String key) {
        try {
            keyLocker.acquire(key);
            return lastModifiedMigrate(key);
        } finally {
            keyLocker.release(key);
        }
    }

    private long lastModifiedOld(String key) {
        return dbManagerV4.lastModified(key);
    }

    private long lastModifiedMigrate(String key){
        return dbManager.lastModified(key);
    }

    List<String> getTotalKey() {
        try {
            // Acquire global lock to make sure per-key operations (delete etc) completed
            // and block future per-key operations until reading for all keys is completed
            keyLocker.acquireGlobal();
            return dbManager.getAllKeys();
        } finally {
            keyLocker.releaseGlobal();
        }
    }

    private List<String> getTotalKeyOld(){
        return dbManagerV4.getAllKeys();
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

    String getDbName() {
        return mDbName;
    }
}

