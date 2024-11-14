package com.serkanerip.stowagecommon;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

public class TransportMessageCodec extends ByteToMessageCodec<TransportMessage> {

    @Override
    protected void encode(
        ChannelHandlerContext channelHandlerContext, TransportMessage message, ByteBuf out
    ) throws Exception {
        out.writeInt(message.getSize());
        out.writeByte(message.getType().getType());
        out.writeLong(message.getCorrelationId());
        out.writeBytes(message.getPayload());
    }

    @Override
    protected void decode(
        ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list
    ) throws Exception {
        // Ensure there's enough bytes to read the size field (4 bytes for an int)
        if (byteBuf.readableBytes() < 4) {
            return; // Not enough data yet, return and wait for more
        }

        byteBuf.markReaderIndex(); // Mark the current reader index

        int size = byteBuf.readInt();

        // Ensure there's enough bytes for the entire frame based on the size
        if (byteBuf.readableBytes() < size - 4) {
            byteBuf.resetReaderIndex(); // Reset to the marked index
            return; // Not enough data yet, return and wait for more
        }

        // Read the frame type
        var type = TransportMessageType.fromByte(byteBuf.readByte());

        // Ensure there's enough bytes for the long (8 bytes)
        if (byteBuf.readableBytes() < 8) {
            byteBuf.resetReaderIndex();
            return; // Not enough data yet, return and wait for more
        }

        var id = byteBuf.readLong();
        ByteBuf payload = byteBuf.readBytes(size - TransportMessage.HEADER_SIZE);

        list.add(new TransportMessage(type, id, payload));
    }
}
