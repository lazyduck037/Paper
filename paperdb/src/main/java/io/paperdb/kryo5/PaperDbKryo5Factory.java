package io.paperdb.kryo5;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

import io.paperdb.PaperTable;
import io.paperdb.serializer.NoArgCollectionSerializer;

/**
 * @author lazyduck037
 */
public class PaperDbKryo5Factory {
    private final HashMap<Class, Serializer> mCustomSerializers;
    public PaperDbKryo5Factory(HashMap<Class, Serializer> customSerializers){
        mCustomSerializers = customSerializers;
    }
    public Kryo createKryoInstance(boolean compatibilityMode) {
        Kryo kryo = new Kryo();

        if (compatibilityMode) {
            kryo.setOptimizedGenerics(true);
        }

        kryo.register(PaperTable.class);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);

        // Serialize Arrays$ArrayList
        //noinspection ArraysAsListWithZeroOrOneArgument
        kryo.register(Arrays.asList("").getClass(), new io.paperdb.serializer.kryo.ArraysAsListSerializer());
        io.paperdb.serializer.kryo.UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        io.paperdb.serializer.kryo.SynchronizedCollectionsSerializer.registerSerializers(kryo);
        // Serialize inner AbstractList$SubAbstractListRandomAccess
        kryo.addDefaultSerializer(new ArrayList<>().subList(0, 0).getClass(),
                new NoArgCollectionSerializer());
        // Serialize AbstractList$SubAbstractList
        kryo.addDefaultSerializer(new LinkedList<>().subList(0, 0).getClass(),
                new NoArgCollectionSerializer());
        // To keep backward compatibility don't change the order of serializers above

        // UUID support
        kryo.register(UUID.class, new io.paperdb.serializer.kryo.UUIDSerializer());

        for (Class<?> clazz : mCustomSerializers.keySet())
            kryo.register(clazz, mCustomSerializers.get(clazz));

        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    }
}
