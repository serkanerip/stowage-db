package com.serkanerip.stowagebenchmark;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.serkanerip.stowageclient.Client;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Benchmark {
    private static final Logger logger = LoggerFactory.getLogger(Benchmark.class);
    private static final String HOST = System.getenv().getOrDefault("KV_STORE_HOST", "localhost");
    private static final int PORT =
        Integer.parseInt(System.getenv().getOrDefault("KV_STORE_PORT", "3030"));

    private final BenchmarkConfiguration config;

    private final Client client;
    private final Thread[] workers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger completedRequests = new AtomicInteger(0);
    private final ThreadLocal<Random> threadLocalRandom = ThreadLocal.withInitial(Random::new);
    private final byte[][] keys;
    private final byte[][] values;
    private static AtomicInteger readCounter = new AtomicInteger(0);
    private static AtomicInteger writeCounter = new AtomicInteger(0);

    Benchmark(BenchmarkConfiguration benchmarkConfiguration) {
        this.config = benchmarkConfiguration;

        logger.info("Connecting to the {}:{}", HOST, PORT);
        this.client = new Client(HOST, PORT);  // Replace with configurable host/port
        this.workers = new Thread[config.threadCount()];
        this.keys = new byte[config.keyCount()][];
        this.values = new byte[config.valueCount()][];
        populateKeysAndValues();
        fillServerWithDesiredEntryCount();
    }

    private void fillServerWithDesiredEntryCount() {
        logger.info("Filling server with {} entry", config.desiredEntryCountBeforeTest());
        try (var es = Executors.newFixedThreadPool(config.threadCount())) {
            SecureRandom random = new SecureRandom();
            var entryCountToCreate =
                Math.min(config.desiredEntryCountBeforeTest(), config.keyCount());
            var countDownLatch = new CountDownLatch(entryCountToCreate);
            for (int i = 0; i < entryCountToCreate; i++) {
                var key = keys[i];
                es.submit(() -> {
                    client.put(key, values[random.nextInt(values.length)]);
                    countDownLatch.countDown();
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for keys to be created", e);
                System.exit(1);
            }
            es.shutdown();
        }
        logger.info("Finished filling server with {} entry", config.desiredEntryCountBeforeTest());
    }

    void populateKeysAndValues() {
        SecureRandom random = new SecureRandom();
        for (int i = 0; i < config.keyCount(); i++) {
            var key = new byte[config.keySize()];
            random.nextBytes(key);
            keys[i] = key;
        }
        for (int i = 0; i < config.valueCount(); i++) {
            var value = new byte[config.valueSize()];
            random.nextBytes(value);
            values[i] = value;
        }
    }

    void runBenchmark() {
        logger.info("Starting benchmark with configuration:");
        printConfiguration();

        // Warm-up phase
        if (config.warmupDurationSeconds() > 0) {
            logger.info("Starting warm-up phase for {} seconds...", config.warmupDurationSeconds());
            runForDuration(config.warmupDurationSeconds(),
                new Histogram(TimeUnit.SECONDS.toNanos(10), 3), true);
            logger.info("Warm-up completed.");
        }

        // Main benchmark
        Histogram latencyHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        long testStartTime = System.nanoTime();

        if (config.durationSeconds() > 0) {
            logger.info("Running time-based test for {} seconds...", config.durationSeconds());
            runForDuration(config.durationSeconds(), latencyHistogram, false);
        } else {
            logger.info("Running request-based test for {} requests...", config.requestCount());
            runForRequestCount(latencyHistogram);
        }

        long duration = System.nanoTime() - testStartTime;
        shutdown();
        printResults(latencyHistogram, duration);
        logger.info("Performed Read count: {} Write count: {}", readCounter.get(), writeCounter.get());

        var result = new ResultManager.BenchmarkResult(
            completedRequests.get(),
            latencyHistogram.getValueAtPercentile(50),
            latencyHistogram.getValueAtPercentile(99),
            latencyHistogram.getValueAtPercentile(99.9),
            latencyHistogram.getValueAtPercentile(99.99),
            latencyHistogram.getMaxValue(),
            completedRequests.get() / (duration / 1e9)
        );

        new ResultManager(config).saveResults(result);
    }

    private void runForDuration(int duration, Histogram histogram, boolean isWarmup) {
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration);
        CountDownLatch latch = new CountDownLatch(config.threadCount());

        for (int i = 0; i < config.threadCount(); i++) {
            workers[i] = Thread.ofVirtual().start(() -> {
                try {
                    while (System.nanoTime() < endTime && running.get()) {
                        processRequest(histogram, isWarmup);
                    }
                } finally {
                    latch.countDown();
                    threadLocalRandom.remove();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Test interrupted", e);
        }
    }

    private void runForRequestCount(Histogram histogram) {
        CountDownLatch latch = new CountDownLatch(config.requestCount());

        for (int i = 0; i < config.threadCount(); i++) {
            workers[i] = Thread.ofVirtual().start(() -> {
                try {
                    for (int c = 0; c < config.requestCount(); c++) {
                        processRequest(histogram, false);
                    }
                } finally {
                    latch.countDown();
                    threadLocalRandom.remove();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Test interrupted", e);
        }
    }

    private void processRequest(Histogram histogram, boolean isWarmup) {
        try {
            long startTime = System.nanoTime();

            Random random = threadLocalRandom.get();
            double action = random.nextDouble();
            var key = keys[random.nextInt(keys.length)];

            if (action < config.readRatio()) {
                readCounter.incrementAndGet();
                client.get(key);
            } else {
                client.put(key, values[random.nextInt(values.length)]);
                writeCounter.incrementAndGet();
            }

            if (!isWarmup) {
                long latency = System.nanoTime() - startTime;
                histogram.recordValue(latency);
                completedRequests.incrementAndGet();
            }
        } catch (Exception e) {
            logger.error("Error processing request", e);
        }
    }

    private void printConfiguration() {
        var message = """
                
                ======== Benchmark Configuration ========
                Host: %s
                Port: %d
                Thread Count: %d
                %s: %s
                Key Size: %d bytes
                Value Size: %d bytes
                Read Ratio: %.2f%%
                Write Ratio: %.2f%%
                Warmup Duration: %d seconds
                =====================================""".formatted(HOST, PORT, config.threadCount(),
            config.durationSeconds() > 0 ? "Test Duration" : "Request Count",
            config.durationSeconds() > 0 ? config.durationSeconds() + " seconds" :
                config.requestCount(),
            config.keySize(), config.valueSize(), config.readRatio(), config.writeRatio(),
            config.warmupDurationSeconds());
        logger.info(message);
    }

    private void printResults(Histogram histogram, long duration) {
        double totalDurationSeconds = duration / 1e9;
        var message = """
                
                ======== Benchmark Results ========
                Total Requests: %d
                P50 Latency: %.2fms
                P95 Latency: %.2fms
                P99 Latency: %.2fms
                P999 Latency: %.2fms
                P9999 Latency: %.2fms
                Maximum Latency: %.2fms
                Total Duration: %.2f seconds
                Throughput: %.2f requests/seconds
                ===================================""".formatted(
        completedRequests.get(), histogram.getValueAtPercentile(50) / 1_000_000.0,
            histogram.getValueAtPercentile(95) / 1_000_000.0,
            histogram.getValueAtPercentile(99) / 1_000_000.0,
            histogram.getValueAtPercentile(99.9) / 1_000_000.0,
            histogram.getValueAtPercentile(99.99) / 1_000_000.0,
            histogram.getMaxValue() / 1_000_000.0, totalDurationSeconds,
            completedRequests.get() / totalDurationSeconds);
        logger.info(message);
    }

    private void shutdown() {
        running.set(false);
        try {
            for (var worker : workers) {
                worker.join(5000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.shutdown();
        logger.info("Benchmark completed.");
    }

    public static void main(String[] args) {
        ConfigurationLoader loader =
            new ConfigurationLoader("./throughput_test.yaml");

        Benchmark benchmark = new Benchmark(loader.load());
        benchmark.runBenchmark();
    }
}