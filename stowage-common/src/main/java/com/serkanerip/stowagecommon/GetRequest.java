package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class GetRequest implements MessagePayload {
    private final HeapData key;

    public GetRequest(HeapData key) {
        this.key = key;
    }

    public static GetRequest decode(ByteBuf buf) {
        var keySize = buf.readInt();
        var key = new byte[keySize];
        buf.readBytes(key);
        return new GetRequest(new HeapData(key));
    }

    public HeapData getKey() {
        return key;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.GET;
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.size());
        buffer.writeBytes(key.toByteArray());
        return buffer;
    }
}
