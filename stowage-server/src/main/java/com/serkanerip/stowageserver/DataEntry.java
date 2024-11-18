package com.serkanerip.stowageserver;

import static com.serkanerip.stowageserver.KeyValueLogStore.TOMBSTONE_MARKER;

import java.nio.ByteBuffer;

import com.serkanerip.stowagecommon.HeapData;

class DataEntry {

    private final HeapData key;

    private final HeapData value;

    private final boolean deleted;

    public DataEntry(HeapData key, HeapData value) {
        this.key = key;
        this.value = value;
        this.deleted = TOMBSTONE_MARKER.equals(value);
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

    public boolean isDeleted() {
        return deleted;
    }

    public HeapData getKey() {
        return key;
    }

    public HeapData getValue() {
        return value;
    }
}
