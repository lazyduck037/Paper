package io.paperdb;
;

class ThreadLocalKryo extends ThreadLocal<com.esotericsoftware.kryo.kryo5.Kryo> {
    private final PaperDbKryo5Factory mFactory;
    ThreadLocalKryo(PaperDbKryo5Factory factory){
        this.mFactory = factory;
    }

    @Override
    protected com.esotericsoftware.kryo.kryo5.Kryo initialValue() {
        return mFactory.createKryoInstance(false);
    }

}
