package com.serkanerip.stowageserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StowageServer {

    private static final Logger logger = LoggerFactory.getLogger(StowageServer.class);
    private final LogStructuredStore store;

    private final NettyServer nettyServer;
    private final StoreOperationHandler storeOperationHandler;
    private final ServerOptions serverOptions;

    public StowageServer(ServerOptions serverOptions) {
        this.serverOptions = serverOptions;
        var segmentStore = DataSegmentStore.create(serverOptions);
        var inMemoryIndex = InMemoryIndex.create(segmentStore);
        this.store = new LogStructuredStore(inMemoryIndex, segmentStore);
        this.storeOperationHandler = new StoreOperationHandler(store);
        this.nettyServer = new NettyServer(storeOperationHandler);
    }

    public void start() {
        logTime("Server start", () -> {
            store.init();
            nettyServer.start(serverOptions.inetHost(), serverOptions.inetPort());
        });
    }

    public void shutdown() {
        logTime("Server shut down", () -> {
            logger.info("Shutting down server");
            nettyServer.shutdown();
            storeOperationHandler.shutdown();
            store.shutdown();
        });
    }

    private void logTime(String action, Runnable runnable) {
        var startTime = System.currentTimeMillis();
        runnable.run();
        logger.info("{} took {} ms", action, System.currentTimeMillis() - startTime);
    }

}
