package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DeleteRequest implements MessagePayload {
    private final HeapData key;

    public DeleteRequest(HeapData key) {
        this.key = key;
    }

    public static DeleteRequest decode(ByteBuf buf) {
        var keySize = buf.readInt();
        var key = new byte[keySize];
        buf.readBytes(key);
        return new DeleteRequest(new HeapData(key));
    }

    public HeapData getKey() {
        return key;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.DELETE;
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.size());
        buffer.writeBytes(key.toByteArray());
        return buffer;
    }
}
