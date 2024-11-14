package com.serkanerip.stowageserver;

import java.io.Serializable;
import java.util.Arrays;

public class Data implements Serializable {
    private final byte[] data;

    public Data(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public int size() {
        return data.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Data that = (Data) obj;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return Arrays.toString(data);
    }
}
