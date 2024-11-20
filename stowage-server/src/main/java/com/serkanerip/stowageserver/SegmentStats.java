package com.serkanerip.stowageserver;

public class SegmentStats {
    long totalKeyCount;
    long totalDataSize;
    long obsoleteKeyCount;
    long obsoleteDataSize;

    public long getTotalKeyCount() {
        return totalKeyCount;
    }

    public long getTotalDataSize() {
        return totalDataSize;
    }

    public long getObsoleteKeyCount() {
        return obsoleteKeyCount;
    }

    public long getObsoleteDataSize() {
        return obsoleteDataSize;
    }

    public double obsoleteKeyRatio() {
        if (obsoleteKeyCount == 0) {
            return 0;
        }
        return (double) obsoleteKeyCount / totalKeyCount;
    }

    public double obsoleteDataRatio() {
        if (obsoleteDataSize == 0) {
            return 0;
        }
        return (double) obsoleteDataSize / totalDataSize;
    }

    @Override
    public String toString() {
        return """
            SegmentStats {
                totalKeyCount=%d, obsoleteKeyCount=%d, obsoleteKeyRatio=%.2f
                totalDataSize=%d, obsoleteDataSize=%d, obsoleteDataRatio=%.2f
            }""".formatted(totalKeyCount, obsoleteKeyCount, obsoleteKeyRatio(),
            totalDataSize, obsoleteDataSize, obsoleteDataRatio());
    }
}
