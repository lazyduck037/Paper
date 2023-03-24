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
    private final boolean mIsMigration;
    private final DbManager5 dbManager;
    private DbManagerV4 dbManagerV4;
    private final KeyLocker keyLocker = new KeyLocker(); // To sync key-dependent operations by key

    private final Operation operation;

    private final boolean mForceUse4;

    private String mDbName;

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
        mDbName = dbName;
        String dbPath = context.getFilesDir() + File.separator + dbName;
        String oldDbPath = null;
        if (forceUse4){
            dbManagerV4 = new DbManagerV4(dbPath, true);
        }else if (isMigration){
            oldDbPath = dbPath;
            dbPath = dbPath + "New";
            dbManagerV4 =  new DbManagerV4(oldDbPath, false);
        }
        dbManager = new DbManager5(dbPath);
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
        mDbName = dbName;
        String dbPath = dbFilesDir + File.separator + dbName;
        String oldDbPath = null;
        if (forceUse4){
            dbManagerV4 = new DbManagerV4(dbPath, true);
        }else if (isMigration){
            oldDbPath = dbPath;
            dbPath = dbPath + "New";
            dbManagerV4 = new DbManagerV4(oldDbPath, false);
        }
        dbManager = new DbManager5(dbPath);
    }

    void destroy() {
        // Acquire global lock to make sure per-key operations (read, write etc) completed
        // and block future per-key operations until destroy is completed
        try {
            keyLocker.acquireGlobal();

            if (mForceUse4){
                destroyForce();
                return;
            }

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

    private void destroyForce(){
        if (!dbManagerV4.destroy()) {
            Log.e(TAG, "Couldn't delete Paper dir " + dbManagerV4.getDbPath());
        }
    }

    <E> void insert(String key, E value) {
        try {
            keyLocker.acquire(key);

            if (mForceUse4){
                insertOld(key, value);
            }else {
                insertNew(key, value);
            }
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
            if (mForceUse4){
                return selectOld(key);
            }
            return selectMigrate(key);
        } finally {
            keyLocker.release(key);
        }
    }

    private <E> E selectOld(String key) {
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

    private <E> E selectMigrate(String key) {
        if (mIsMigration){
            dbManager.assertInit();
            final File originalFile = dbManager.getOriginalFile(key);
            final File backupFile = dbManager.makeBackupFile(originalFile);
            if(!dbManager.createBackUpBeforeSelect(originalFile, backupFile, key)){
                return selectOld(key);
            }
            return operation.readTableFile(key, originalFile);

        }else {
            return selectNew(key);
        }
    }

    boolean exists(String key) {
        try {
            keyLocker.acquire(key);
            if (mForceUse4){
                return existOld(key);
            }
            return existMigrate(key);
        } finally {
            keyLocker.release(key);
        }
    }

    private boolean existMigrate(String key){
        if (mIsMigration){
            boolean res = dbManager.existsInternal(key);
            if (!res){
                return dbManagerV4.existsInternal(key);
            }
            return true;
        }
        return dbManager.existsInternal(key);
    }

    private boolean existOld(String key){
        return dbManagerV4.existsInternal(key);
    }

    long lastModified(String key) {
        try {
            keyLocker.acquire(key);
            if (mForceUse4){
                return lastModifiedOld(key);
            }
            return lastModifiedMigrate(key);
        } finally {
            keyLocker.release(key);
        }
    }

    private long lastModifiedOld(String key){
        return dbManagerV4.lastModified(key);
    }

    private long lastModifiedMigrate(String key){
        if (mIsMigration){
            if(dbManager.existsInternal(key)){
                return dbManager.lastModified(key);
            }
            return dbManagerV4.lastModified(key);
        }
        return dbManager.lastModified(key);
    }

    List<String> getTotalKey() {
        try {
            // Acquire global lock to make sure per-key operations (delete etc) completed
            // and block future per-key operations until reading for all keys is completed
            keyLocker.acquireGlobal();
            if (mForceUse4){
                return getTotalKeyOld();
            }
            return getTotalKeyMigrate();
        } finally {
            keyLocker.releaseGlobal();
        }
    }

    private List<String> getTotalKeyMigrate(){
        List<String> currentDb = dbManager.getAllKeys();
        List<String> oldDb;
        if (mIsMigration){
            oldDb = dbManagerV4.getAllKeys();
        }else {
            oldDb = new LinkedList<>();
        }
        HashSet<String> res = new HashSet<>(currentDb);
        res.addAll(oldDb);
        return new ArrayList(res);
    }

    private List<String> getTotalKeyOld(){
        return dbManagerV4.getAllKeys();
    }

    void deleteIfExists(String key) {
        try {
            keyLocker.acquire(key);
            if (mForceUse4){
                deleteIfExistOld(key);
            }else {
                deleteIfExistMigrate(key);
            }
        } finally {
            keyLocker.release(key);
        }
    }

    private void deleteIfExistOld(String key){
        dbManagerV4.deleteTable(key);
    }

    private void deleteIfExistMigrate(String key){
        if(mIsMigration){
            dbManagerV4.deleteTable(key);
        }
        dbManager.deleteTable(key);
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

    String getDbName(){
        if (mForceUse4){
            return mDbName;
        }
        if (mIsMigration){
            return mDbName + "New";
        }
        return mDbName;
    }
}

