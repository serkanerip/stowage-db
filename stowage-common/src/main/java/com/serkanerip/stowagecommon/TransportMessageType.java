package com.serkanerip.stowagecommon;

public enum TransportMessageType {
    HEARTBEAT((byte) 0x00),
    SIMPLE_RESPONSE((byte) 0x01),
    PUT((byte) 0x02),
    GET((byte) 0x03),
    GET_RESPONSE((byte) 0x04),
    DELETE((byte) 0x05);

    private final byte type;

    TransportMessageType(byte b) {
        this.type = b;
    }

    public byte getType() {
        return type;
    }

    public static TransportMessageType fromByte(byte b) {
        return switch (b) {
            case 0x00 -> HEARTBEAT;
            case 0x01 -> SIMPLE_RESPONSE;
            case 0x02 -> PUT;
            case 0x03 -> GET;
            case 0x04 -> GET_RESPONSE;
            case 0x05 -> DELETE;
            default -> throw new IllegalArgumentException("Invalid frame type: " + b);
        };
    }
}
