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
        StowageDbCodec.encode(message, out);
    }

    @Override
    protected void decode(
        ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list
    ) throws Exception {
        var message = StowageDbCodec.decode(byteBuf);
        if (message == null) {
            return;
        }
        list.add(message);
    }
}
