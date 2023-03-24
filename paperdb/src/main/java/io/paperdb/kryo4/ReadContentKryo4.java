package io.paperdb.kryo4;

import com.esotericsoftware.kryo.io.Input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import io.paperdb.PaperTable;

/**
 * @author lazyduck037
 */
public class ReadContentKryo4 {
   final ThreadLocalKryo4 mKryo;
   public ReadContentKryo4(ThreadLocalKryo4 kryo){
      mKryo = kryo;
   }

   public <E> E readContentRetry(File originalFile) throws FileNotFoundException, com.esotericsoftware.kryo.kryo5.KryoException {

      final Input i = new Input(new FileInputStream(originalFile));
      //noinspection TryFinallyCanBeTryWithResources
      try {
         //noinspection unchecked
         final PaperTable<E> paperTable = mKryo.createKryToRetry().readObject(i, PaperTable.class);
         return paperTable.get();
      } finally {
         i.close();
      }
   }

   public <E> E readContent(File originalFile) throws FileNotFoundException, com.esotericsoftware.kryo.KryoException {
      final Input i = new Input(new FileInputStream(originalFile));
      //noinspection TryFinallyCanBeTryWithResources
      try {
         //noinspection unchecked
         final PaperTable<E> paperTable = mKryo.get().readObject(i, PaperTable.class);
         return paperTable.get();
      } finally {
         i.close();
      }
   }
}
