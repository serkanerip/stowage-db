package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DeleteRequest implements MessagePayload {
    private final byte[] key;

    public DeleteRequest(byte[] key) {
        this.key = key;
    }

    public static DeleteRequest decode(ByteBuf buf) {
        var keySize = buf.readInt();
        var key = new byte[keySize];
        buf.readBytes(key);
        return new DeleteRequest(key);
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.DELETE;
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeInt(key.length);
        buffer.writeBytes(key);
        return buffer;
    }
}
