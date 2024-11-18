package com.serkanerip.stowagebenchmark;

import java.util.HashMap;
import java.util.Map;

record BenchmarkConfiguration(
    int threadCount,
    int durationSeconds,
    int warmupDurationSeconds,
    int keySize,
    int valueSize,
    double readRatio,
    double writeRatio,
    int requestCount
) {

    Map<String, Object> toMap() {
        return new HashMap<>(){
            {
                put("threadCount", threadCount);
                put("durationSeconds", durationSeconds);
                put("warmupDurationSeconds", warmupDurationSeconds);
                put("keySize", keySize);
                put("valueSize", valueSize);
                put("readRatio", readRatio);
                put("writeRatio", writeRatio);
                put("requestCount", requestCount);
            }
        };
    }
}
