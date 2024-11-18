package com.serkanerip.stowagebenchmark;

import java.util.HashMap;
import java.util.Map;

record BenchmarkConfiguration(
    int threadCount,
    int durationSeconds,
    int warmupDurationSeconds,
    int keySize,
    int keyCount,
    int valueSize,
    int valueCount,
    double readRatio,
    double writeRatio,
    int requestCount
) {

    Map<String, Object> toMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("threadCount", threadCount);
        configMap.put("durationSeconds", durationSeconds);
        configMap.put("warmupDurationSeconds", warmupDurationSeconds);
        configMap.put("keySize", keySize);
        configMap.put("keyCount", keyCount);
        configMap.put("valueSize", valueSize);
        configMap.put("valueCount", valueCount);
        configMap.put("readRatio", readRatio);
        configMap.put("writeRatio", writeRatio);
        configMap.put("requestCount", requestCount);
        return configMap;
    }
}
