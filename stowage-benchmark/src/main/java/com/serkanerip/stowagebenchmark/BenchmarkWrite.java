package com.serkanerip.stowagebenchmark;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.serkanerip.stowageclient.Client;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BenchmarkWrite {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkWrite.class);
    private static final String HOST = System.getenv().getOrDefault("KV_STORE_HOST", "localhost");
    private static final int PORT =
        Integer.parseInt(System.getenv().getOrDefault("KV_STORE_PORT", "3030"));


    public static void main(String[] args) {
        var threadCount = 1;
        CountDownLatch latch = new CountDownLatch(threadCount);
        var client = new Client(HOST, PORT);

        Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        var workers = new Thread[threadCount];
        var fixedValue = new byte[1024];
        new Random().nextBytes(fixedValue);
        for (int i = 0; i < threadCount; i++) {
            workers[i] = Thread.ofVirtual().start(() -> {
                try {
                    for (int c = 0; c < threadCount; c++) {
                        for (int j = 6111258; j < Integer.MAX_VALUE; j++) {
                            var key = "KEY_KEY_KEY-" + j;
                            long startTime = System.nanoTime();
                            client.put(key.getBytes(StandardCharsets.UTF_8), fixedValue);
                            long latency = System.nanoTime() - startTime;
                            histogram.recordValue(latency);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Test interrupted", e);
        }

        System.out.println("""
            
            ======== Benchmark Results ========
            P50 Latency: %.2fms
            P95 Latency: %.2fms
            P99 Latency: %.2fms
            P999 Latency: %.2fms
            P9999 Latency: %.2fms
            Maximum Latency: %.2fms
            ===================================""".formatted(
            histogram.getValueAtPercentile(50) / 1_000_000.0,
            histogram.getValueAtPercentile(95) / 1_000_000.0,
            histogram.getValueAtPercentile(99) / 1_000_000.0,
            histogram.getValueAtPercentile(99.9) / 1_000_000.0,
            histogram.getValueAtPercentile(99.99) / 1_000_000.0,
            histogram.getMaxValue() / 1_000_000.0));
        client.shutdown();
    }
}