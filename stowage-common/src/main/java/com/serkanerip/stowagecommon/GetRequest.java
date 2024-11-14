package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class GetRequest implements MessagePayload {
    private final byte[] key;

    public GetRequest(byte[] key) {
        this.key = key;
    }

    public static GetRequest decode(ByteBuf buf) {
        var keySize = buf.readInt();
        var key = new byte[keySize];
        buf.readBytes(key);
        return new GetRequest(key);
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.GET;
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.length);
        buffer.writeBytes(key);
        return buffer;
    }
}
