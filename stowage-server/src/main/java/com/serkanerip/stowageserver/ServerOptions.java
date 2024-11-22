package com.serkanerip.stowageserver;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public record ServerOptions(
    String inetHost, int inetPort, Path dataRootPath, Double compactionThreshold,
    long maxFileSize, long flushDataSize
) {
    private static final String ENV_PREFIX = "STOWAGE_";
    private static final String PROP_PREFIX = "stowage.";

    // Default values stored in a map with field name as key
    private static final Map<String, Object> DEFAULT_VALUES = Map.of(
        "inetHost", "localhost",
        "inetPort", 3030,
        "dataRootPath", "./stowage-data",
        "compactionThreshold", 0.5,
        "maxFileSize", 1024L * 1024 * 1024,
        "flushDataSize", 10L * 1024 * 1024
    );

    public static ServerOptions fromEnvironmentOrProperties() {
        var constructor = ServerOptions.class.getConstructors()[0];
        var parameters = constructor.getParameters();
        var args = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            var param = parameters[i];
            var fieldName = param.getName();
            var defaultValue = DEFAULT_VALUES.get(fieldName);
            var envValue = getEnvOrProperty(toEnvName(fieldName), String.valueOf(defaultValue));
            args[i] = convertValue(envValue, param.getType());
        }

        try {
            return (ServerOptions) constructor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ServerOptions", e);
        }
    }

    private static String getEnvOrProperty(String fieldName, String defaultValue) {
        String envVar = ENV_PREFIX + fieldName;
        String propertyKey = PROP_PREFIX + fieldName.toLowerCase();

        return Optional.ofNullable(System.getenv(envVar))
            .orElse(Optional.ofNullable(System.getProperty(propertyKey))
                .orElse(defaultValue));
    }

    private static String toEnvName(String fieldName) {
        // Convert camelCase to SCREAMING_SNAKE_CASE
        return fieldName.replaceAll("([a-z])([A-Z]+)", "$1_$2").toUpperCase();
    }

    private static Object convertValue(String value, Class<?> targetType) {
        if (targetType == String.class) {
            return value;
        } else if (targetType == int.class || targetType == Integer.class) {
            return Integer.parseInt(value);
        } else if (targetType == long.class || targetType == Long.class) {
            return Long.parseLong(value);
        } else if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(value);
        } else if (targetType == Path.class) {
            return Path.of(value);
        } else if (targetType == double.class || targetType == Double.class) {
            return Double.parseDouble(value);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + targetType);
        }
    }

    // Builder pattern for programmatic configuration
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String inetHost = (String) DEFAULT_VALUES.get("inetHost");
        private int inetPort = (int) DEFAULT_VALUES.get("inetPort");
        private Path dataRootPath = Path.of((String) DEFAULT_VALUES.get("dataRootPath"));
        private double compactionThreshold = (double) DEFAULT_VALUES.get("compactionThreshold");
        private long maxFileSize = (long) DEFAULT_VALUES.get("maxFileSize");
        private long flushDataSize = (long) DEFAULT_VALUES.get("flushDataSize");

        public Builder inetHost(String inetHost) {
            this.inetHost = inetHost;
            return this;
        }

        public Builder inetPort(int inetPort) {
            this.inetPort = inetPort;
            return this;
        }

        public Builder maxFileSize(long size) {
            this.maxFileSize = size;
            return this;
        }

        public Builder flushDataSize(long size) {
            this.flushDataSize = size;
            return this;
        }

        public Builder compactionThreshold(double compactionThreshold) {
            this.compactionThreshold = compactionThreshold;
            return this;
        }

        public Builder dataRootPath(Path dataRootPath) {
            this.dataRootPath = dataRootPath;
            return this;
        }

        public ServerOptions build() {
            return new ServerOptions(
                inetHost, inetPort, dataRootPath, compactionThreshold, maxFileSize, flushDataSize
            );
        }
    }
}
