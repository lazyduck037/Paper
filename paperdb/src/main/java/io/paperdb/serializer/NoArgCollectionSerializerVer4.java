package io.paperdb.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author lazyduck037
 */
public class NoArgCollectionSerializerVer4 extends CollectionSerializer {
   @Override
   protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
      return new ArrayList<>();
   }
}
