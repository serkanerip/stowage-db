package com.serkanerip.stowagecommon;

import java.io.Serializable;
import java.util.Arrays;

public class HeapData implements Serializable {
    private final byte[] payload;

    public HeapData(byte[] payload) {
        this.payload = payload;
    }

    public byte[] toByteArray() {
        return payload;
    }

    public int size() {
        return payload.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HeapData that = (HeapData) obj;
        if (that.size() != size()) {
            return false;
        }
        return Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return Arrays.toString(payload);
    }
}
