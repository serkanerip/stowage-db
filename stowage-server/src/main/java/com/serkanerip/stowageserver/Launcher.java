package com.serkanerip.stowageserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private Launcher() {}

    public static void main(String[] args) {
        var options = ServerOptions.fromEnvironmentOrProperties();
        logger.info("Starting server with options: {}", options);
        logTime("Server start", () -> {
            var db = new StowageDB(options);
            var inboundHandler = new ServerInboundHandler(db);
            var nettyServer = new NettyServer(inboundHandler);
            nettyServer.start(options.inetHost(), options.inetPort());    
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logTime("Server shut down", () -> {
                    logger.info("Shutting down server");
                    nettyServer.shutdown();
                    db.shutdown();
                });
            }));
        });
    }

    private static void logTime(String action, Runnable runnable) {
        var startTime = System.currentTimeMillis();
        runnable.run();
        logger.info("{} took {} ms", action, System.currentTimeMillis() - startTime);
    }

}
