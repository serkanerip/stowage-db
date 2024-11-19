package com.serkanerip.stowagebenchmark;

import java.io.FileInputStream;
import java.util.Properties;

class ConfigurationLoader {
    private final Properties properties;

    public ConfigurationLoader(String configPath) {
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
        } catch (Exception e) {
            throw new ConfigurationFileReadFailedException(e);
        }
    }

    public int getDesiredEntryCountBeforeTest() {
        return Integer.parseInt(properties.getOrDefault("desired_entry_count_before_test", "0").toString());
    }

    public int getThreadCount() {
        return Integer.parseInt(properties.getOrDefault("threads", "4").toString());
    }

    public int getDurationSeconds() {
        String durationStr = properties.getOrDefault("duration", "60").toString();
        return parseDurationString(durationStr);
    }

    public int getWarmupDuration() {
        String durationStr = properties.getOrDefault("warmup_duration", "10").toString();
        return parseDurationString(durationStr);
    }

    private int parseDurationString(String durationStr) {
        durationStr = durationStr.toUpperCase();
        if (durationStr.endsWith("S")) {
            return Integer.parseInt(durationStr.substring(0, durationStr.length() - 1));
        }
        if (durationStr.endsWith("M")) {
            return Integer.parseInt(durationStr.substring(0, durationStr.length() - 1)) * 60;
        }
        return Integer.parseInt(durationStr);
    }

    public int getRequestCount() {
        return Integer.parseInt(properties.getOrDefault("request_count", "100000").toString());
    }

    public int getKeySize() {
        return Integer.parseInt(properties.getOrDefault("key_size", "16").toString());
    }

    public int getKeyCount() {
        return Integer.parseInt(properties.getOrDefault("key_count", "10000").toString());
    }

    public int getValueSize() {
        return Integer.parseInt(properties.getOrDefault("value_size", "128").toString());
    }

    public int getValueCount() {
        return Integer.parseInt(properties.getOrDefault("value_count", "10000").toString());
    }

    public double getReadRatio() {
        return Double.parseDouble(properties.getOrDefault("read_ratio", "0.8").toString());
    }

    public double getWriteRatio() {
        return Double.parseDouble(properties.getOrDefault("write_ratio", "0.2").toString());
    }

    public BenchmarkConfiguration load() {
        return new BenchmarkConfiguration(
            getThreadCount(),
            getDurationSeconds(), getWarmupDuration(),
            getKeySize(), getKeyCount(),
            getDesiredEntryCountBeforeTest(),
            getValueSize(), getValueCount(),
            getReadRatio(), getWriteRatio(),
            getRequestCount()
        );
    }
}
