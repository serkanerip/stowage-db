package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;

public class StowageDbCodec {

    private StowageDbCodec() {}

    public static void encode(TransportMessage message, ByteBuf out) {
        out.writeInt(message.getSize());
        out.writeByte(message.getType().getType());
        out.writeLong(message.getCorrelationId());
        out.writeBytes(message.getPayload());
    }

    public static TransportMessage decode(ByteBuf byteBuf) {
        TransportMessage message = null;
        // Ensure there's enough bytes to read the size field (4 bytes for an int)
        if (byteBuf.readableBytes() < 4) {
            return message; // Not enough data yet, return and wait for more
        }

        byteBuf.markReaderIndex(); // Mark the current reader index

        int size = byteBuf.readInt();

        // Ensure there's enough bytes for the entire frame based on the size
        if (byteBuf.readableBytes() < size - 4) {
            byteBuf.resetReaderIndex(); // Reset to the marked index
            return message; // Not enough data yet, return and wait for more
        }

        // Read the frame type
        var type = TransportMessageType.fromByte(byteBuf.readByte());

        // Ensure there's enough bytes for the long (8 bytes)
        if (byteBuf.readableBytes() < 8) {
            byteBuf.resetReaderIndex();
            return message; // Not enough data yet, return and wait for more
        }

        var id = byteBuf.readLong();
        ByteBuf payload = byteBuf.readBytes(size - TransportMessage.HEADER_SIZE);

        message = new TransportMessage(type, id, payload);
        return message;
    }
}
