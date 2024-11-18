package com.serkanerip.stowageserver;

class SegmentStats {
    long totalKeyCount;
    long totalDataSize;
    long obsoleteKeyCount;
    long obsoleteDataSize;

    double obsoleteKeyRatio() {
        if (obsoleteKeyCount == 0) {
            return 0;
        }
        return (double) obsoleteKeyCount / totalKeyCount;
    }

    double obsoleteDataRatio() {
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
