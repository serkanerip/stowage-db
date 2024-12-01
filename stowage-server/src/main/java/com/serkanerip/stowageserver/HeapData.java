package com.serkanerip.stowageserver;

import java.io.Serializable;
import java.util.Arrays;

class HeapData implements Serializable {
    private final byte[] payload;

    HeapData(byte[] payload) {
        this.payload = payload;
    }

    int size() {
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
