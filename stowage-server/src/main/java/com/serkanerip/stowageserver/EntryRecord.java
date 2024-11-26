package com.serkanerip.stowageserver;

import java.nio.ByteBuffer;

class EntryRecord {

    private final byte[] key;

    private final byte[] value;

    public EntryRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer serialize() {
        var keySize = key.length;
        var valueSize = value.length;
        var buff = ByteBuffer.allocate(4 + keySize + valueSize + 4);
        buff.putInt(keySize);
        buff.put(key);
        buff.putInt(valueSize);
        buff.put(value);
        buff.flip();
        return buff;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
