package com.serkanerip.stowageclient;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.serkanerip.stowagecommon.MessagePayload;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NettyClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private EventLoopGroup workerGroup;
    private Channel clientChannel;
    private final Thread monitoringThread;
    private static final AtomicLong correlationId = new AtomicLong(0);

    private final ConcurrentMap<Long, OngoingRequest> requestsMap = new ConcurrentHashMap<>();

    public NettyClient() {
        this.monitoringThread = Thread.ofVirtual().unstarted(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);
                    logger.info("Number of ongoing requests: {}", requestsMap.size());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Monitoring thread interrupted");
                    break;
                }
            }
        });
    }

    void start(String host, int port) {
        this.monitoringThread.start();
        workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                            .addLast(new TransportMessageCodec(), new ClientHandler(requestsMap));
                    }
                });

            clientChannel = b.connect(host, port).syncUninterruptibly().channel();
            logger.info("Connected to the {}:{}", host, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    OngoingRequest sendMessage(MessagePayload payload) {
        if (clientChannel == null || !clientChannel.isActive()) {
            // TODO find a better way of handling this
            throw new RuntimeException("Connection to server is not active. Unable to send message.");
        }
        var id = correlationId.incrementAndGet();
        OngoingRequest request = new OngoingRequest(id);
        requestsMap.put(id, request);
        var buf = payload.encode();
        var message = new TransportMessage(payload.getType(), id, buf);
        logger.debug("Sending message of type: {} with size: {} corId: {}", message.getType(),
            message.getSize(), message.getCorrelationId());
        clientChannel.writeAndFlush(message);
        return request;
    }


    void shutdown() {
        this.monitoringThread.interrupt();
        if (clientChannel != null) {
            clientChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        logger.info("Shutdown complete.");
    }

}
