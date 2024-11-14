package com.serkanerip.stowagecommon;

import java.util.Optional;

public enum ErrorCode {
    NO_ERR((short) 0x00),
    INVALID_FRAME((short) 0x01);

    private final short value;

    ErrorCode(short i) {
        this.value = i;
    }

    public short getValue() {
        return value;
    }

    public static Optional<ErrorCode> valueOf(short val) {
        for (var code : values()) {
            if (code.value == val) {
                return Optional.of(code);
            }
        }
        return Optional.empty();
    }
}
