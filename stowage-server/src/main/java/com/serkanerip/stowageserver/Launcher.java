package com.serkanerip.stowageserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private Launcher() {}

    public static void main(String[] args) {
        var startTime = System.currentTimeMillis();
        var options = ServerOptions.fromEnvironmentOrProperties();
        logger.info("Starting server with options: {}", options);
        var db = new StowageDB(options);
        var inboundHandler = new ServerInboundHandler(db);
        var nettyServer = new NettyServer(inboundHandler);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            var startTime1 = System.currentTimeMillis();
            logger.info("Shutting down server");
            nettyServer.shutdown();
            db.shutdown();
            logger.info("Server shut down took {} ms", System.currentTimeMillis() - startTime1);
        }));
        try {
            nettyServer.start(options.inetHost(), options.inetPort());
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            System.exit(1);
        }
        logger.info("Server start took {} ms", System.currentTimeMillis() - startTime);
    }

}
