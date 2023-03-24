package io.paperdb;

import static io.paperdb.Utils.sync;

import com.esotericsoftware.kryo.kryo5.Kryo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import io.paperdb.kryo4.ReadContentKryo4;
import io.paperdb.kryo5.ReadContentKryo5;

/**
 * @author lazyduck037
 */
class Operation {
    private final ReadContentKryo5 mKryo5;
    private final ReadContentKryo4 mKryo4;

    Operation(ReadContentKryo5 kryo5, ReadContentKryo4 kryo4, boolean isMigrate){
        mKryo4 = kryo4;
        mKryo5 = kryo5;
    }

    <E> E readTableFile(String key, File originalFile) {
        try {
            return mKryo5.readContent(originalFile);
        } catch (FileNotFoundException | com.esotericsoftware.kryo.kryo5.KryoException | ClassCastException e) {
            Throwable exception = e;
            // Give one more chance, read data in paper 1.x compatibility mode
            if (e instanceof com.esotericsoftware.kryo.kryo5.KryoException) {
                try {
                    return mKryo5.readContentRetry(originalFile);
                } catch (FileNotFoundException | com.esotericsoftware.kryo.kryo5.KryoException | ClassCastException compatibleReadException) {
                    exception = compatibleReadException;
                }
            }
            String errorMessage = "Couldn't read/deserialize file "
                    + originalFile + " for table " + key;
            throw new PaperDbException(errorMessage, exception);
        }
    }

    <E> E readTableFileV4(String key, File originalFile) {
        try {
            return mKryo4.readContent(originalFile);
        } catch (FileNotFoundException | com.esotericsoftware.kryo.KryoException | ClassCastException e) {
            Throwable exception = e;
            // Give one more chance, read data in paper 1.x compatibility mode
            if (e instanceof com.esotericsoftware.kryo.KryoException) {
                try {
                    return mKryo4.readContentRetry(originalFile);
                } catch (FileNotFoundException | com.esotericsoftware.kryo.KryoException | ClassCastException compatibleReadException) {
                    exception = compatibleReadException;
                }
            }
            String errorMessage = "Couldn't read/deserialize file "
                    + originalFile + " for table " + key;
            throw new PaperDbException(errorMessage, exception);
        }
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
    public  <E> void writeTableFile(String key, PaperTable<E> paperTable,
                                    File originalFile, File backupFile) {
        com.esotericsoftware.kryo.kryo5.io.Output kryoOutput = null;
        try {
            FileOutputStream fileStream = new FileOutputStream(originalFile);
            kryoOutput = new com.esotericsoftware.kryo.kryo5.io.Output(fileStream);
            getKryo5().writeObject(kryoOutput, paperTable);
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
     * Attempt to write the file, delete the backup and return true as atomically as
     * possible.  If any exception occurs, delete the new file; next time we will restore
     * from the backup.
     *
     * @param key          table key
     * @param paperTable   table instance
     * @param originalFile file to write new data
     * @param backupFile   backup file to be used if write is failed
     */
    public <E> void writeTableFileV4(String key, PaperTable<E> paperTable,
                                      File originalFile, File backupFile) {
        com.esotericsoftware.kryo.io.Output kryoOutput = null;
        try {
            FileOutputStream fileStream = new FileOutputStream(originalFile);
            kryoOutput = new com.esotericsoftware.kryo.io.Output(fileStream);
            getKryo4().writeObject(kryoOutput, paperTable);
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

    public Kryo getKryo5(){
        return mKryo5.getKryo();
    }

    public com.esotericsoftware.kryo.Kryo getKryo4(){
        return mKryo4.getKryo();
    }
}
