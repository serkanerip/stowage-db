package com.serkanerip.stowageserver;

import static com.serkanerip.stowageserver.KeyValueLogStore.TOMBSTONE_MARKER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

class DataEntry {

    private final Data key;

    private final Data value;

    private final boolean deleted;

    public DataEntry(Data key, Data value) {
        this.key = key;
        this.value = value;
        this.deleted = Arrays.equals(TOMBSTONE_MARKER, value.getData());
    }

    public int persistTo(FileChannel fc) throws IOException {
        var keySize = key.size();
        var valueSize = value.size();
        var buff = ByteBuffer.allocateDirect(4 + keySize + valueSize + 4);
        buff.putInt(keySize);
        buff.put(key.getData());
        buff.putInt(valueSize);
        buff.put(value.getData());
        buff.flip();
        return fc.write(buff, fc.size());
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }
}
