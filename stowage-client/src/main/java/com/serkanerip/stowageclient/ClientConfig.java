package com.serkanerip.stowageclient;

public record ClientConfig(
    String host, int port, int requestTimeout,
    RetryPolicy connectRetryPolicy
) {

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

    public record RetryPolicy(int maxRetries, int baseBackoffMillis) {

        public RetryPolicy {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("Max retries must be a positive number");
            }
            if (baseBackoffMillis < 0) {
                throw new IllegalArgumentException("Base backoff millis must be a positive number");
            }
        }
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 3065;
        private int requestTimeout = 5000;
        private RetryPolicy connectRetryPolicy = new RetryPolicy(3, 500);

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

        /**
         * Sets the retry policy for connection attempts, including an exponential backoff strategy for delays.
         *
         * @param retryPolicy the {@link RetryPolicy} instance specifying:
         *                    - The maximum number of retry attempts.
         *                    - The base delay (in milliseconds) between attempts, which will be
         *                      exponentially increased using the formula:
         *                      <pre>
         *                          delay = Math.min(baseBackoffMillis * (2^(attempt-1)), 10000);
         *                      </pre>
         *                      The delay is capped at a maximum of 10,000 milliseconds (10 seconds).
         * @return the current Builder instance for chaining.
         * Example:
         * <pre>
         *     RetryPolicy retryPolicy = new RetryPolicy(5, 500); // 5 retries with exponential backoff
         *     new Builder().connectRetryPolicy(retryPolicy).build();
         * </pre>
         *
         * Notes:
         * - Ensure the {@code retryPolicy} values are meaningful. For instance:
         *   - Retries should be >= 0.
         *   - Base delay should be a positive integer.
         * - The exponential backoff prevents overwhelming the server by progressively increasing wait time
         *   between attempts, but with a reasonable cap to maintain responsiveness.
         */
        public Builder connectRetryPolicy(RetryPolicy retryPolicy) {
            this.connectRetryPolicy = retryPolicy;
            return this;
        }

        public ClientConfig build() {
            return new ClientConfig(host, port, requestTimeout, connectRetryPolicy);
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
