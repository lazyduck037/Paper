package io.paperdb.kryo4;

import com.esotericsoftware.kryo.Kryo;

public class ThreadLocalKryo4 extends ThreadLocal<Kryo> {
    private final PaperDbKryo4Factory mFactory;

    public ThreadLocalKryo4(PaperDbKryo4Factory factory) {
        this.mFactory = factory;
    }

    @Override
    protected Kryo initialValue() {
        return createKryo(false);
    }

    public Kryo createKryToRetry() {
        return createKryo(true);
    }

    private Kryo createKryo(boolean compatibilityMode) {
        return mFactory.createKryoInstance(compatibilityMode);
    }
}