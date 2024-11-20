package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PutRequest implements MessagePayload {
    private final byte[] key;
    private final byte[] value;

    public PutRequest(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
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
        return new PutRequest(key, value);
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.length);
        buffer.writeBytes(key);
        buffer.writeInt(value.length);
        buffer.writeBytes(value);
        return buffer;
    }
}
