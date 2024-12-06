package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class SimpleResponse implements MessagePayload {
    public static final SimpleResponse SR_SUCCESS = new SimpleResponse(true, ErrorCode.NO_ERR);
    private final boolean success;
    private final ErrorCode errorCode;
    private final ByteBuf cache;

    public SimpleResponse(boolean success, ErrorCode errorCode) {
        this.success = success;
        this.errorCode = errorCode;
        this.cache = encodeInternal();
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
        return cache.duplicate();
    }

    private ByteBuf encodeInternal() {
        var buffer = Unpooled.buffer(
            1 + Short.BYTES, 1 + Short.BYTES
        );
        buffer.writeBoolean(success);
        buffer.writeShort(errorCode.getValue());
        return buffer;
    }
}
