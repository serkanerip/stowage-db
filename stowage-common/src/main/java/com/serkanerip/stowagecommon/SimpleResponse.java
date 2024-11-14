package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class SimpleResponse implements MessagePayload {
    private final boolean success;

    private final ErrorCode errorCode;

    public SimpleResponse(boolean success, ErrorCode errorCode) {
        this.success = success;
        this.errorCode = errorCode;
    }

    public static SimpleResponse decode(ByteBuf buf) {
        var success = buf.readBoolean();
        var errorCode = ErrorCode.valueOf(buf.readShort()).orElseThrow();
        return new SimpleResponse(success, errorCode);
    }

    public boolean isSuccess() {
        return success;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public TransportMessageType getType() {
        return TransportMessageType.SIMPLE_RESPONSE;
    }

    @Override
    public ByteBuf encode() {
        var buffer = Unpooled.buffer();
        buffer.writeBoolean(success);
        buffer.writeShort(errorCode.getValue());
        return buffer;
    }
}
