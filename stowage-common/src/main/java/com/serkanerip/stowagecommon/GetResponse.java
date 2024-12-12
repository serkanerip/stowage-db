package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class GetResponse implements MessagePayload {
    private final byte[] value;

    public GetResponse(byte[] value) {
        this.value = value;
    }

    public static GetResponse decode(ByteBuf buf) {
        var valueSize = buf.readInt();
        if (valueSize == 0) {
            return new GetResponse(null);
        }
        var b = new byte[valueSize];
        buf.readBytes(b);
        return new GetResponse(b);
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.GET_RESPONSE;
    }

    @Override
    public ByteBuf encode() {
        ByteBuf buffer = Unpooled.buffer(Integer.BYTES);
        if (value == null) {
            buffer.writeInt(0);
            return buffer;
        }

        buffer.writeInt(value.length);
        ByteBuf valueBuf = Unpooled.wrappedBuffer(value); // Wrap without copying
        return Unpooled.wrappedBuffer(buffer, valueBuf);  // Combine buffers
    }
}
