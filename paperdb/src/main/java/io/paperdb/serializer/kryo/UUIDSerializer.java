package io.paperdb.serializer.kryo;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

import java.util.UUID;

public class UUIDSerializer extends Serializer<UUID> {

   public UUIDSerializer() {
      setImmutable(true);
   }

   @Override
   public void write(final Kryo kryo, final Output output, final UUID uuid) {
      output.writeLong(uuid.getMostSignificantBits());
      output.writeLong(uuid.getLeastSignificantBits());
   }

   @Override public UUID read(final Kryo kryo, final Input input, final Class<? extends UUID> uuidClass) {
      return new UUID(input.readLong(), input.readLong());
   }
}
