package com.serkanerip.stowageserver;

import java.nio.file.Path;
import java.util.Optional;

public record ServerOptions(String inetHost, int inetPort, Path dataRootPath) {

    private static final String ENV_PREFIX = "STOWAGE_";
    private static final String PROP_PREFIX = "stowage.";

    private static final String DEFAULT_INET_HOST = "localhost";
    private static final int DEFAULT_INET_PORT = 3030;
    private static final String DEFAULT_DATA_ROOT_PATH = "./stowage-data";

    public static ServerOptions fromEnvironmentOrProperties() {
        return new ServerOptions(
            getEnvOrProperty("INET_HOST", DEFAULT_INET_HOST),
            Integer.parseInt(getEnvOrProperty("INET_PORT", String.valueOf(DEFAULT_INET_PORT))),
            Path.of(getEnvOrProperty("DATA_ROOT_PATH", DEFAULT_DATA_ROOT_PATH))
        );
    }

    private static String getEnvOrProperty(String fieldName, String defaultValue) {
        // Construct environment variable and property names
        String envVar = ENV_PREFIX + fieldName;
        String propertyKey = PROP_PREFIX + fieldName.toLowerCase();

        // Retrieve the value from the environment, properties, or default
        return Optional.ofNullable(System.getenv(envVar))
            .orElse(Optional.ofNullable(System.getProperty(propertyKey))
                .orElse(defaultValue));
    }
}
