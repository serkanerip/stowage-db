package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PutRequest implements MessagePayload {
    private final HeapData key;
    private final HeapData value;

    public PutRequest(HeapData key, HeapData value) {
        this.key = key;
        this.value = value;
    }

    public HeapData getKey() {
        return key;
    }

    public HeapData getValue() {
        return value;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.PUT;
    }

    public static PutRequest decode(ByteBuf buf) {
        var keySize = buf.readInt();
        var key = new byte[keySize];
        buf.readBytes(key);
        var valueSize = buf.readInt();
        var value = new byte[valueSize];
        buf.readBytes(value);
        return new PutRequest(new HeapData(key), new HeapData(value));
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.size());
        buffer.writeBytes(key.toByteArray());
        buffer.writeInt(value.size());
        buffer.writeBytes(value.toByteArray());
        return buffer;
    }
}
