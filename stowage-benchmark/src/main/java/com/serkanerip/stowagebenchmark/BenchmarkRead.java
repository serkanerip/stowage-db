package com.serkanerip.stowagebenchmark;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.serkanerip.stowageclient.Client;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BenchmarkRead {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkRead.class);
    private static final String HOST = System.getenv().getOrDefault("KV_STORE_HOST", "localhost");
    private static final int PORT =
        Integer.parseInt(System.getenv().getOrDefault("KV_STORE_PORT", "3030"));


    public static void main(String[] args) {
        var threadCount = 32;
        CountDownLatch latch = new CountDownLatch(threadCount);
        var client = new Client(HOST, PORT);
        var durationToRun = 30 * 1000;
        var keys = new byte[13_000_000][];
        for (int i = 0; i < keys.length; i++) {
            var key = ("KEY_KEY_KEY-" + i).getBytes(StandardCharsets.UTF_8);
            keys[i] = key;
        }

        Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        var workers = new Thread[threadCount];
        var totalRequestCount = new AtomicLong();
        for (int i = 0; i < threadCount; i++) {
            workers[i] = Thread.ofVirtual().name("benchmark", i).start(() -> {
                var random = new Random();
                try {
                    var endTime = System.currentTimeMillis() + durationToRun;
                    while (!Thread.interrupted() && System.currentTimeMillis() < endTime) {
                        var key = keys[random.nextInt(keys.length)];
                        long latencyStartTime = System.nanoTime();
                        client.get(key);
                        long latency = System.nanoTime() - latencyStartTime;
                        histogram.recordValue(latency);
                        totalRequestCount.incrementAndGet();
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
            Total requests: %d
            P50 Latency: %.2fms
            P95 Latency: %.2fms
            P99 Latency: %.2fms
            P999 Latency: %.2fms
            P9999 Latency: %.2fms
            Maximum Latency: %.2fms
            ===================================""".formatted(
                totalRequestCount.get(),
            histogram.getValueAtPercentile(50) / 1_000_000.0,
            histogram.getValueAtPercentile(95) / 1_000_000.0,
            histogram.getValueAtPercentile(99) / 1_000_000.0,
            histogram.getValueAtPercentile(99.9) / 1_000_000.0,
            histogram.getValueAtPercentile(99.99) / 1_000_000.0,
            histogram.getMaxValue() / 1_000_000.0)
        );
        client.shutdown();
    }
}