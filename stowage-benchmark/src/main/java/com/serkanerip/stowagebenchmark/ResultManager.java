package com.serkanerip.stowagebenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

class ResultManager {
    private static final Logger logger = LoggerFactory.getLogger(ResultManager.class);

    private static final String RESULTS_DIR = "benchmark_results";
    private final BenchmarkConfiguration benchmarkConfiguration;

    public ResultManager(BenchmarkConfiguration config) {
        this.benchmarkConfiguration = config;
    }

    public void saveResults(BenchmarkResult result) {
        try {
            Files.createDirectories(Paths.get(RESULTS_DIR));

            String resultId = generateResultId();
            String filename = generateResultFilename(resultId);

            Map<String, Object> resultMetadata = new HashMap<>();
            resultMetadata.put("result_id", resultId);
            resultMetadata.put("timestamp", LocalDateTime.now().toString());
            resultMetadata.put("config", benchmarkConfiguration.toMap());
            resultMetadata.put("metrics", result.getMetrics());

            DumperOptions options = new DumperOptions();
            options.setIndent(2);
            options.setPrettyFlow(true);
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(options);
            try (FileWriter writer = new FileWriter(Paths.get(RESULTS_DIR, filename).toFile())) {
                yaml.dump(resultMetadata, writer);
            }

            logger.info("Benchmark result saved to {}", filename);
        } catch (IOException e) {
            logger.error("Failed to save benchmark result", e);
        }
    }

    private String generateResultId() {
        return UUID.randomUUID().toString();
    }

    private String generateResultFilename(String resultId) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format("benchmark_result_%s_%s.yaml", timestamp, resultId.substring(0, 8));
    }

    public static class BenchmarkResult {
        private final Map<String, Object> metrics;

        public BenchmarkResult(
            long totalRequests,
            double p50Latency,
            double p99Latency,
            double p999Latency,
            double p9999Latency,
            double maxLatency,
            double throughput
        ) {
            metrics = new HashMap<>();
            metrics.put("totalRequests", totalRequests);
            metrics.put("throughput", throughput);
            metrics.put("p50Latency", p50Latency);
            metrics.put("p99Latency", p99Latency);
            metrics.put("p999Latency", p999Latency);
            metrics.put("p9999Latency", p9999Latency);
            metrics.put("maxLatency", maxLatency);
        }

        public Map<String, Object> getMetrics() {
            return new HashMap<>(metrics);
        }
    }
}