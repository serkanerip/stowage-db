package com.serkanerip.stowagecommon;

import io.netty.buffer.ByteBuf;

/**
 * Represents a transport message with a structured header and payload.
 *
 * <p><b>Header</b></p>
 * <ul>
 *   <li><b>size</b> (4 bytes): The total message size.</li>
 *   <li><b>type</b> (1 byte): The type of the message.</li>
 *   <li><b>UUID</b> (36 bytes): The correlation ID for tracking the message.</li>
 * </ul>
 *
 * <p><b>Payload</b></p>
 * <p>The message body, containing the actual data. The length is variable.</p>
 */
public class TransportMessage {

    public static final int HEADER_SIZE = 13;

    private final int size;

    private final TransportMessageType transportMessageType;

    private final long corId;

    // TODO possible memory leak
    private final ByteBuf payload;

    public TransportMessage(TransportMessageType transportMessageType, long corId, ByteBuf payload) {
        this.size = HEADER_SIZE + payload.readableBytes();
        this.corId = corId;
        this.payload = payload;
        this.transportMessageType = transportMessageType;
    }

    public long getCorrelationId() {
        return corId;
    }

    public TransportMessageType getType() {
        return transportMessageType;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    public int getSize() {
        return size;
    }
}
