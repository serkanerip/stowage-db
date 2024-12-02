package com.serkanerip.stowageclient;

public record ClientConfig(String host, int port, int requestTimeout) {

    public ClientConfig {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (requestTimeout < 0) {
            throw new IllegalArgumentException("Request timeout must be a positive number");
        }
    }

    public static ClientConfig.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 3065;
        private int requestTimeout = 5000;

        /**
         * Sets the host for the stowage server.
         *
         * @param host the hostname or IP address of the server (e.g., "localhost" or "127.0.0.1").
         * @return the current Builder instance for chaining.
         * Example:
         * <pre>
         *     new Builder().host("192.168.1.100").build();
         * </pre>
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the port for stowage server.
         *
         * @param port the port number to connect to (default: 3065).
         * @return the current Builder instance for chaining.
         * Example:
         * <pre>
         *     new Builder().port(8080).build();
         * </pre>
         *
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the request timeout for the client-server communication.
         *
         * @param requestTimeout the timeout in milliseconds (e.g., 5000 for 5 seconds).
         * @return the current Builder instance for chaining.
         * Example:
         * <pre>
         *     new Builder().requestTimeout(10000).build();
         * </pre>
         *
         * Tip:
         * Choose a value appropriate for the expected server response time.
         * A higher value can help in cases of network latency but may delay error detection.
         */
        public Builder requestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(host, port, requestTimeout);
        }
    }

    @Override
    public String toString() {
        return """
            ClientConfig{
                host='%s',
                port=%d,
                requestTimeout=%d
            }""".formatted(
            host, port, requestTimeout
        );
    }
}
