package com.serkanerip.stowageserver;

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

class StoreQueue {
    private final Logger logger = LoggerFactory.getLogger(StoreQueue.class);
    private final AtomicLong handledRequestCount = new AtomicLong(0);
    private final AtomicLong receivedRequestCount = new AtomicLong(0);
    private final Thread thread;
    private final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    private volatile boolean isQueueClosed = false;
    private final KeyValueLogStore store;

    StoreQueue(KeyValueLogStore store) {
        this.store = store;
        thread = Thread.ofVirtual().start(this::processQueue);
    }

    void shutdown() {
        isQueueClosed = true;
        logger.info("Shutting down!");
        var localReceivedCount = receivedRequestCount.get();
        while (localReceivedCount > handledRequestCount.get()) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                logger.warn(
                    "Waiting for all tasks to be completed interrupted! remainingTaskCount={}",
                    localReceivedCount - handledRequestCount.get());
                break;
            }
        }
        logger.info("All tasks completed, shutting down!");
        thread.interrupt();
    }

    private void processQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            Runnable runnable;
            try {
                runnable = queue.take();
                runnable.run();
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public void addToQueue(Runnable runnable) {
        queue.add(runnable);
    }

    public void addToQueue(MessagePayload messagePayload, long corId, Channel channel) {
        if (isQueueClosed) {
            logger.warn(
                "Queue is closed, executor will shutdown soon therefore no new task allowed!");
            return;
        }
        try {
            receivedRequestCount.incrementAndGet();
            queue.put(() -> {
                try {
                    executePayload(messagePayload, corId, channel);
                } catch (RuntimeException re) {
                    logger.error("Failed to execute request {}", corId, re);
                } finally {
                    handledRequestCount.incrementAndGet();
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to put the runnable in queue, thread interrupted!",
                e);
        }
    }

    private void executePayload(MessagePayload messagePayload, long corId, Channel channel) {
        logger.debug("Handling request of type: {} with corId: {}",
            messagePayload.getClass().getSimpleName(), corId);
        switch (messagePayload) {
            case PutRequest request -> {
                store.put(request.getKey(), request.getValue());
                var simpleResponse = new SimpleResponse(true, ErrorCode.NO_ERR);
                if (channel.isActive()) {
                    channel.writeAndFlush(
                        new TransportMessage(TransportMessageType.SIMPLE_RESPONSE, corId,
                            simpleResponse.encode()));
                } else {
                    logger.warn("Channel is not active, can't send the response!");
                }
            }
            case GetRequest request -> {
                var value = store.get(request.getKey());
                var response = new GetResponse(value);
                if (channel.isActive()) {
                    var frame = new TransportMessage(TransportMessageType.GET_RESPONSE, corId,
                        response.encode());
                    channel.writeAndFlush(frame);
                } else {
                    logger.warn("Channel is not active, can't send the response!");
                }
            }
            case DeleteRequest request -> {
                store.delete(request.getKey());
                var simpleResponse = new SimpleResponse(true, ErrorCode.NO_ERR);
                if (channel.isActive()) {
                    channel.writeAndFlush(
                        new TransportMessage(TransportMessageType.SIMPLE_RESPONSE, corId,
                            simpleResponse.encode()));
                } else {
                    logger.warn("Channel is not active, can't send the response!");
                }
            }
            default -> {
                logger.warn("Unknown frame type!");
                channel.close();
            }
        }
    }

}
