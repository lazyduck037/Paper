package io.paperdb.kryo4;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.paperdb.PaperTable;
import io.paperdb.serializer.NoArgCollectionSerializerVer4;


/**
 * @author lazyduck037
 */
public class PaperDbKryo4Factory {
    private final HashMap<Class, Serializer> mCustomSerializers;
    public PaperDbKryo4Factory(HashMap<Class, Serializer> customSerializers){
        mCustomSerializers = customSerializers;
    }
    public Kryo createKryoInstance(boolean compatibilityMode) {
        Kryo kryo = new Kryo();

        if (compatibilityMode) {
            kryo.getFieldSerializerConfig().setOptimizedGenerics(true);
        }

        kryo.register(PaperTable.class);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.setReferences(false);

        // Serialize Arrays$ArrayList
        //noinspection ArraysAsListWithZeroOrOneArgument
        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        // Serialize inner AbstractList$SubAbstractListRandomAccess
        kryo.addDefaultSerializer(new ArrayList<>().subList(0, 0).getClass(),
                new NoArgCollectionSerializerVer4());
        // Serialize AbstractList$SubAbstractList
        kryo.addDefaultSerializer(new LinkedList<>().subList(0, 0).getClass(),
                new NoArgCollectionSerializerVer4());
        // To keep backward compatibility don't change the order of serializers above

        // UUID support
        kryo.register(UUID.class, new UUIDSerializer());

        for (Class<?> clazz : mCustomSerializers.keySet())
            kryo.register(clazz, mCustomSerializers.get(clazz));

        kryo.setInstantiatorStrategy(
                new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    }
}
