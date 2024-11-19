package com.serkanerip.stowageserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.ErrorCode;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.MessagePayload;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageType;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StoreOperationHandler {
    private final Logger logger = LoggerFactory.getLogger(StoreOperationHandler.class);
    private final AtomicLong handledRequestCount = new AtomicLong(0);
    private final AtomicLong receivedRequestCount = new AtomicLong(0);
    private final Thread writeThread;
    private final ExecutorService readExecutor;
    private final LinkedBlockingQueue<Runnable> writeQueue = new LinkedBlockingQueue<>();
    private volatile boolean isQueueClosed = false;
    private final LogStructuredStore store;
    private final Object writeLock = new Object();

    StoreOperationHandler(LogStructuredStore store) {
        this.store = store;
        this.readExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.writeThread = Thread.ofVirtual()
            .name("writer-thread")
            .start(this::processWriteQueue);
    }

    void shutdown() {
        isQueueClosed = true;
        logger.info("Shutting down!");
        var localReceivedCount = receivedRequestCount.get();

        // Wait for all tasks to complete
        while (localReceivedCount > handledRequestCount.get()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn(
                    "Waiting for all tasks to be completed interrupted! remainingTaskCount={}",
                    localReceivedCount - handledRequestCount.get());
                break;
            }
        }

        readExecutor.shutdown();
        writeThread.interrupt();
        logger.info("All tasks completed, shutdown successful!");
    }

    private void processWriteQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Runnable runnable = writeQueue.take();
                runnable.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Write queue thread interrupted!");
                break;
            }
        }
    }

    public void addToQueue(MessagePayload messagePayload, long corId, Channel channel) {
        if (isQueueClosed) {
            logger.warn(
                "Queue is closed, executor will shutdown soon therefore no new task allowed!");
            return;
        }

        receivedRequestCount.incrementAndGet();

        if (messagePayload instanceof GetRequest request) {
            readExecutor.execute(() -> {
                try {
                    executeReadRequest(request, corId, channel);
                } catch (RuntimeException re) {
                    logger.error("Failed to execute read request {}", corId, re);
                } finally {
                    handledRequestCount.incrementAndGet();
                }
            });
        } else if (messagePayload instanceof PutRequest
            || messagePayload instanceof DeleteRequest) {
            try {
                writeQueue.put(() -> {
                    try {
                        executeWriteOrDeleteRequest(messagePayload, corId, channel);
                    } catch (RuntimeException re) {
                        logger.error("Failed to execute write/delete request {}", corId, re);
                    } finally {
                        handledRequestCount.incrementAndGet();
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Failed to process request, thread interrupted!", e);
            }
        } else {
            logger.warn("Unknown message payload type!");
            channel.close();
            handledRequestCount.incrementAndGet();
        }
    }

    private void executeReadRequest(GetRequest request, long corId, Channel channel) {
        logger.debug("Handling read request with corId: {}", corId);
        var value = store.get(request.getKey());
        var response = new GetResponse(value);
        sendResponse(channel, new TransportMessage(
            TransportMessageType.GET_RESPONSE,
            corId,
            response.encode()
        ));
    }

    private void executeWriteOrDeleteRequest(MessagePayload messagePayload, long corId,
                                             Channel channel) {
        logger.debug("Handling write/delete request of type: {} with corId: {}",
            messagePayload.getClass().getSimpleName(), corId);

        switch (messagePayload) {
            case PutRequest request -> {
                synchronized (writeLock) {
                    store.put(request.getKey(), request.getValue());
                }
                sendResponse(channel, new TransportMessage(
                    TransportMessageType.SIMPLE_RESPONSE,
                    corId,
                    new SimpleResponse(true, ErrorCode.NO_ERR).encode()
                ));
            }
            case DeleteRequest request -> {
                synchronized (writeLock) {
                    store.delete(request.getKey());
                }
                sendResponse(channel, new TransportMessage(
                    TransportMessageType.SIMPLE_RESPONSE,
                    corId,
                    new SimpleResponse(true, ErrorCode.NO_ERR).encode()
                ));
            }
            default -> {
                logger.warn("Unknown write/delete payload type!");
                channel.close();
            }
        }

    }

    private void sendResponse(Channel channel, TransportMessage message) {
        if (channel.isActive()) {
            channel.writeAndFlush(message);
        } else {
            logger.warn("Channel is not active, can't send the response!");
        }
    }
}