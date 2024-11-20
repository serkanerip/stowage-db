package com.serkanerip.stowageserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private final StowageDB store;
    private final NettyServer nettyServer;
    private final ServerOptions serverOptions;

    public static void main(String[] args) {
        var options = ServerOptions.fromEnvironmentOrProperties();
        var launcher = new Launcher(options);
        launcher.start();

        Runtime.getRuntime().addShutdownHook(new Thread(launcher::shutdown));
    }

    public Launcher(ServerOptions serverOptions) {
        this.serverOptions = serverOptions;
        this.store = new StowageDB(serverOptions);
        var inboundHandler = new ServerInboundHandler(store);
        this.nettyServer = new NettyServer(inboundHandler);
    }

    public void start() {
        logTime("Server start", () -> {
            logger.info("Starting server with options: {}", serverOptions);
            nettyServer.start(serverOptions.inetHost(), serverOptions.inetPort());
        });
    }

    public void shutdown() {
        logTime("Server shut down", () -> {
            logger.info("Shutting down server");
            nettyServer.shutdown();
            store.shutdown();
        });
    }

    private void logTime(String action, Runnable runnable) {
        var startTime = System.currentTimeMillis();
        runnable.run();
        logger.info("{} took {} ms", action, System.currentTimeMillis() - startTime);
    }

}
