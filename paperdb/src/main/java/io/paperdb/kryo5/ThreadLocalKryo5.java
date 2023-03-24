package io.paperdb.kryo5;

public class ThreadLocalKryo5 extends ThreadLocal<com.esotericsoftware.kryo.kryo5.Kryo> {
    private final PaperDbKryo5Factory mFactory;

    public ThreadLocalKryo5(PaperDbKryo5Factory factory) {
        this.mFactory = factory;
    }

    @Override
    protected com.esotericsoftware.kryo.kryo5.Kryo initialValue() {
        return createKryo(false);
    }

    public com.esotericsoftware.kryo.kryo5.Kryo createKryToRetry() {
        return createKryo(true);
    }

    private com.esotericsoftware.kryo.kryo5.Kryo createKryo(boolean compatibilityMode) {
        return mFactory.createKryoInstance(compatibilityMode);
    }
}