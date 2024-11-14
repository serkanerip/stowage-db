package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;

public interface MessagePayload {
    TransportMessageType getType();

    ByteBuf encode();
}
