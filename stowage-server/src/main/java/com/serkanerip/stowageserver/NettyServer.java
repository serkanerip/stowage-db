package com.serkanerip.stowageserver;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.serkanerip.stowagecommon.TransportMessageCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    private final ChannelInboundHandler inboundHandler;

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private Channel serverChannel;
    private final ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    NettyServer(ChannelInboundHandler inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    void start(String inetHost, int port) {
        bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            private static final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return Thread.ofPlatform()
                    .name("stowage-server-boss", counter.getAndIncrement())
                    .unstarted(r);
            }
        });
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
            new ThreadFactory() {
            private static final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return Thread.ofPlatform()
                    .name("stowage-server-worker", counter.getAndIncrement())
                    .unstarted(r);
            }
        });
        logger.info("Starting the Netty server with {} loops for bossGroup and {} loops for workerGroup",
            bossGroup.executorCount(), workerGroup.executorCount()
        );
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    logger.debug("Initializing channel {}", ch);
                    ch.pipeline().addLast(
                        new TransportMessageCodec(), inboundHandler
                    );
                    clientChannels.add(ch);
                }
            });

        // Bind and start to accept incoming connections asynchronously
        serverChannel = b.bind(inetHost, port).syncUninterruptibly().channel();
        logger.info("Server started and listening on port {}", port);
    }

    void shutdown() {
        clientChannels.close();
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        logger.info("Netty server stopped");
    }
}
