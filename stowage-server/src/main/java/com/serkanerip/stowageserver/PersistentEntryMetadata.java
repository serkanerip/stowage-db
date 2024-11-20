package com.serkanerip.stowageserver;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

record PersistentEntryMetadata(
    byte[] key,
    int valueSize,
    long valueOffset
) {

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + key.length + Long.BYTES + Integer.BYTES);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putLong(valueOffset);
        buffer.putInt(valueSize);
        buffer.flip();
        return buffer;
    }

    public static PersistentEntryMetadata deserialize(ByteBuffer buffer) {
        int keyLen = buffer.getInt();
        byte[] key = new byte[keyLen];
        buffer.get(key);
        long valueOffset = buffer.getLong();
        int valueSize = buffer.getInt();
        return new PersistentEntryMetadata(key, valueSize, valueOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PersistentEntryMetadata that = (PersistentEntryMetadata) o;
        return valueSize == that.valueSize && valueOffset == that.valueOffset
            && Objects.deepEquals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), valueSize, valueOffset);
    }
}
