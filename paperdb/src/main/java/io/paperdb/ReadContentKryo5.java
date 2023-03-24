package io.paperdb;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author lazyduck037
 */
class ReadContentKryo5 {
   final ThreadLocal<com.esotericsoftware.kryo.kryo5.Kryo> mKryo;
   ReadContentKryo5(ThreadLocal<com.esotericsoftware.kryo.kryo5.Kryo> kryo){
      mKryo = kryo;
   }

   <E> E readContentRetry(File originalFile, Kryo kryo)
           throws FileNotFoundException, com.esotericsoftware.kryo.kryo5.KryoException {

      final Input i = new Input(new FileInputStream(originalFile));
      //noinspection TryFinallyCanBeTryWithResources
      try {
         //noinspection unchecked
         final PaperTable<E> paperTable = kryo.readObject(i, PaperTable.class);
         return paperTable.mContent;
      } finally {
         i.close();
      }
   }

   <E> E readContent(File originalFile)
           throws FileNotFoundException, com.esotericsoftware.kryo.kryo5.KryoException {

      final Input i = new Input(new FileInputStream(originalFile));
      //noinspection TryFinallyCanBeTryWithResources
      try {
         //noinspection unchecked
         final PaperTable<E> paperTable = mKryo.get().readObject(i, PaperTable.class);
         return paperTable.mContent;
      } finally {
         i.close();
      }
   }
}
