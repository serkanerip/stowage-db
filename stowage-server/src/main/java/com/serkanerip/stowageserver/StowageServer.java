package com.serkanerip.stowageserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StowageServer {

    private static final Logger logger = LoggerFactory.getLogger(StowageServer.class);
    private final KeyValueLogStore store;

    private final SegmentMergeWorker compactionTask;
    private final NettyServer nettyServer;
    private final StoreQueue storeQueue;
    private final ServerOptions serverOptions;

    public StowageServer(ServerOptions serverOptions) {
        this.serverOptions = serverOptions;
        var loader = new DataSegmentsLoader(serverOptions);
        this.store = new KeyValueLogStore(serverOptions, loader.load());
        this.storeQueue = new StoreQueue(store);
        this.compactionTask = new SegmentMergeWorker(storeQueue, serverOptions, store);
        this.nettyServer = new NettyServer(storeQueue);
    }

    public void start() {
        logTime("Server start", () -> {
            store.init();
            compactionTask.start();
            nettyServer.start(serverOptions.inetHost(), serverOptions.inetPort());
        });
    }

    public void shutdown() {
        logTime("Server shut down", () -> {
            logger.info("Shutting down server");
            nettyServer.shutdown();
            compactionTask.shutdown();
            storeQueue.shutdown();
            store.shutdown();
        });
    }

    private void logTime(String action, Runnable runnable) {
        var startTime = System.currentTimeMillis();
        runnable.run();
        logger.info("{} took {} ms", action, System.currentTimeMillis() - startTime);
    }

}
