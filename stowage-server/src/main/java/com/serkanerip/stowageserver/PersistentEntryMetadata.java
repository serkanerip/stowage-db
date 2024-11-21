package com.serkanerip.stowageserver;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

record PersistentEntryMetadata(
    byte[] key,
    int valueSize,
    long valueOffset,
    long sequenceNumber
) {

    private static final int KEY_LENGTH_SIZE = Integer.BYTES; // For the length of the key
    private static final int VALUE_OFFSET_SIZE = Long.BYTES;
    private static final int VALUE_SIZE_SIZE = Integer.BYTES;
    private static final int SEQUENCE_NUMBER_SIZE = Long.BYTES;

    public ByteBuffer serialize() {
        int totalSize = KEY_LENGTH_SIZE + key.length + VALUE_OFFSET_SIZE + VALUE_SIZE_SIZE + SEQUENCE_NUMBER_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putLong(valueOffset);
        buffer.putInt(valueSize);
        buffer.putLong(sequenceNumber);
        buffer.flip();
        return buffer;
    }

    public static PersistentEntryMetadata deserialize(ByteBuffer buffer) {
        int keyLen = buffer.getInt();
        byte[] key = new byte[keyLen];
        buffer.get(key);
        long valueOffset = buffer.getLong();
        int valueSize = buffer.getInt();
        long sequenceNumber = buffer.getLong();
        return new PersistentEntryMetadata(key, valueSize, valueOffset, sequenceNumber);
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
