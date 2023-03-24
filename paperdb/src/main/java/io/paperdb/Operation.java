package io.paperdb;

import com.esotericsoftware.kryo.kryo5.Kryo;

import java.io.File;
import java.io.FileNotFoundException;
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

    public Kryo getKryo(){
        return mKryo5.getKryo();
    }
}
