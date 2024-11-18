package com.serkanerip.stowageserver;

import java.nio.ByteBuffer;

record EntryMetadata(
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

    public static EntryMetadata deserialize(ByteBuffer buffer) {
        int keyLen = buffer.getInt();
        byte[] key = new byte[keyLen];
        buffer.get(key);
        long valueOffset = buffer.getLong();
        int valueSize = buffer.getInt();
        return new EntryMetadata(key, valueSize, valueOffset);
    }
}
