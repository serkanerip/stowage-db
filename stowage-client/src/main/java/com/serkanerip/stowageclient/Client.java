package com.serkanerip.stowageclient;

import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.MessagePayload;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class Client extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;
    private final long requestTimeoutInMillis;
    private Channel clientChannel;
    private final Thread monitoringThread;
    private static final AtomicLong correlationId = new AtomicLong(0);
    private final ConcurrentMap<Long, CompletableFuture<MessagePayload>> requestsMap
        = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown = false;

    private final String host;
    private final int port;

    public Client(String host, int port, long requestTimeoutInMillis) {
        this.host = host;
        this.port = port;
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.monitoringThread = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (clientChannel == null || !clientChannel.isActive()) {
                    return;
                }
                try {
                    Thread.sleep(15000);
                    logger.info("Number of ongoing requests: {}", requestsMap.size());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Monitoring thread interrupted");
                    break;
                }
            }
        });
        this.workerGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new TransportMessageCodec(), new InboundHandler());
                }
            });
        validateHostAndPort(Objects.requireNonNull(host), port);
        connectToServer();
    }

    public Client(String host, int port) {
        this(host, port, 5000);
    }

    public synchronized void shutdown() {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;
        if (clientChannel != null) {
            clientChannel.close().awaitUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        this.monitoringThread.interrupt();
        logger.info("Shutdown complete.");
    }

    public byte[] get(byte[] key) {
        var request = new GetRequest(Objects.requireNonNull(key, "Key cannot be null"));
        var future = sendMessage(request);
        try {
            var responsePayload = future.get();
            if (responsePayload instanceof GetResponse response) {
                return response.getValue();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new RequestTimeoutException("Failed to get response from server in time");
            }
            throw new RuntimeException(e);
        }
    }

    public boolean isActive() {
        return clientChannel != null && clientChannel.isActive();
    }

    public boolean delete(byte[] key) {
        var request = new DeleteRequest(Objects.requireNonNull(key, "Key cannot be null"));
        var future = sendMessage(request);
        try {
            var responsePayload = future.get();
            if (responsePayload instanceof SimpleResponse response) {
                return response.isSuccess();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new RequestTimeoutException("Failed to get response from server in time");
            }
            throw new RuntimeException(e);
        }
    }

    public boolean put(byte[] key, byte[] value) {
        var payload = new PutRequest(
            Objects.requireNonNull(key, "Key cannot be null"),
            Objects.requireNonNull(value, "Value cannot be null")
        );
        var future = sendMessage(payload);
        try {
            var responsePayload = future.get();
            if (responsePayload instanceof SimpleResponse sr) {
                return sr.isSuccess();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new RequestTimeoutException("Failed to get response from server in time");
            }
            throw new RuntimeException(e);
        }
    }

    private void validateHostAndPort(String host, int port) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
    }

    private synchronized void connectToServer() {
        if (clientChannel != null && clientChannel.isActive()) {
            return;
        }
        int maxRetries = 3;
        int baseBackoffMillis = 500; // 0.5 second base backoff
        int attempt = 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                clientChannel = bootstrap.connect(host, port).sync().channel();
                logger.info("Connected to the {}:{}", host, port);
                break;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Client interrupted while connecting to server!");
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    var message = "Failed to connect to %s:%d after %d attempts".formatted(host, port, maxRetries);
                    throw new ClientConnectionException(message, e);
                }
                int backoffMillis = calculateBackoff(baseBackoffMillis, attempt);
                logger.warn("Attempt {}/{} to connect to {}:{} failed. Retrying in {} ms.",
                    attempt, maxRetries, host, port, backoffMillis, e);
                sleep(backoffMillis);
            }
        }
    }

    private int calculateBackoff(int baseBackoffMillis, int attempt) {
        // Exponential backoff with a maximum limit
        return Math.min(baseBackoffMillis * (1 << (attempt - 1)), 10000); // Cap at 10 seconds
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry backoff interrupted", e);
        }
    }

    private CompletableFuture<MessagePayload> sendMessage(MessagePayload payload) {
        if (clientChannel == null || !clientChannel.isActive()) {
            connectToServer();
        }
        var id = correlationId.incrementAndGet();
        var future = new CompletableFuture<MessagePayload>();
        requestsMap.put(id, future);
        var buf = payload.encode();
        var message = new TransportMessage(payload.getType(), id, buf);
        logger.debug("Sending message of type: {} with size: {} corId: {}", message.getType(),
            message.getSize(), message.getCorrelationId());
        clientChannel.writeAndFlush(message).addListener(f -> {
            if (f.isSuccess()) {
                return;
            }
            if (f.cause() instanceof ClosedChannelException) {
                future.completeExceptionally(f.cause());
            }
        });
        return future.orTimeout(requestTimeoutInMillis, TimeUnit.MILLISECONDS);
    }

    public static class RequestTimeoutException extends RuntimeException {
        public RequestTimeoutException(String message) {
            super(message);
        }
    }

    private class InboundHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            logger.info("Channel is active!");
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (shuttingDown) {
                return;
            }
            logger.info("Connection to server is closed unexpectedly!");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            TransportMessage transportMessage = (TransportMessage) msg;
            logger.debug("Received message of type: {} with size: {} uuid: {}",
                transportMessage.getType(), transportMessage.getSize(), transportMessage.getCorrelationId());
            var uuid = transportMessage.getCorrelationId();
            var future = requestsMap.remove(uuid);
            if (future != null) {
                var framePayload = switch (transportMessage.getType()) {
                    case SIMPLE_RESPONSE -> SimpleResponse.decode(transportMessage.getPayload());
                    case GET_RESPONSE -> GetResponse.decode(transportMessage.getPayload());
                    default -> throw new IllegalStateException("Unexpected value: " + transportMessage.getType());
                };
                future.complete(framePayload);
            } else {
                logger.error("Received response for unknown request with uuid: {}", uuid);
            }
            transportMessage.getPayload().release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Unexpected exception from downstream", cause);
            ctx.close();
        }

    }

    public static class ClientConnectionException extends RuntimeException {
        public ClientConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }


}
