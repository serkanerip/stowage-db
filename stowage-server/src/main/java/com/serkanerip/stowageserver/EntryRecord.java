package com.serkanerip.stowageserver;

import java.nio.ByteBuffer;

import com.serkanerip.stowagecommon.HeapData;

class EntryRecord {

    private final HeapData key;

    private final HeapData value;

    public EntryRecord(HeapData key, HeapData value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer serialize() {
        var keySize = key.size();
        var valueSize = value.size();
        var buff = ByteBuffer.allocateDirect(4 + keySize + valueSize + 4);
        buff.putInt(keySize);
        buff.put(key.toByteArray());
        buff.putInt(valueSize);
        buff.put(value.toByteArray());
        buff.flip();
        return buff;
    }

    public HeapData getKey() {
        return key;
    }

    public HeapData getValue() {
        return value;
    }
}
