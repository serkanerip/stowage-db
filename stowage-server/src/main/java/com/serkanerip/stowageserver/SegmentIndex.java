package com.serkanerip.stowageserver;

import java.io.Serializable;

public class SegmentIndex implements Serializable {
    private final int valueSize;
    private final long valueOffset;

    public SegmentIndex(int valueSize, long valueOffset) {
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValueOffset() {
        return valueOffset;
    }
}
