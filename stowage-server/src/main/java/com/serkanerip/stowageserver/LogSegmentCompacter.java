package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogSegmentCompacter {

    private static final Logger logger = LoggerFactory.getLogger(LogSegmentCompacter.class);
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private final Thread thread;
    private final LogStructuredStore store;
    private volatile boolean running = true;
    private static final String STOP_FLAG = "STOWAGE_STOP_QUEUE";

    LogSegmentCompacter(LogStructuredStore store) {
        this.store = store;
        this.thread = Thread.ofPlatform()
            .name("compaction")
            .start((this::processQueue));
    }

    void offer(String segmentId) {
        logger.info("Offering segment {}", segmentId);
        if (!queue.contains(segmentId) && !queue.offer(segmentId)) {
                logger.warn("Could not add segment {} to queue", segmentId);
            }
    }

    void shutdown() {
        running = false;
        queue.add(STOP_FLAG);
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processQueue() {
        while (running) {
            try {
                var segmentId = queue.take();
                if (segmentId.equals(STOP_FLAG)) {
                    continue;
                }
                compactWithLock(store.getSegment(segmentId));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while compacting segment", e);
                running = false;
            }
        }
    }

    private void compactWithLock(LogSegment segment) {
        var lock = store.getWriteLock();
        lock.lock();
        try {
            compactSegment(segment);
        } finally {
            lock.unlock();
        }
    }

    private void compactSegment(LogSegment segment) {
        if (segment == null) {
            logger.warn("Segment is null");
            return;
        }
        var startTime = System.currentTimeMillis();
        logger.info("Compacting segment {}", segment.getId());
        var indexIterator = segment.newIndexIterator();
        var segmentDch = segment.getDataChannel();
        var inMemoryIndex = store.getInMemoryIndex();
        var activeSegment = store.getActiveSegment();
        while (indexIterator.hasNext()) {
            var metadata = indexIterator.next();
            var keyHeapData = new HeapData(metadata.key());
            var inMemoryMetadata = inMemoryIndex.get(keyHeapData);
            var isMetadataFresh = inMemoryMetadata.segmentId().equals(segment.getId())
                && inMemoryMetadata.valueOffset() == metadata.valueOffset();
            if (isMetadataFresh) {
                try {
                    var newDataEntry = activeSegment.transferFrom(segmentDch, metadata);
                    inMemoryIndex.put(
                        keyHeapData,
                        InMemoryIndex.MemoryEntryMetadata.fromPersistedEntryMetadata(
                            activeSegment.getId(), newDataEntry
                        )
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        store.decommission(segment.getId());
        logger.info("Compacted segment {} in {} ms", segment.getId(),
            System.currentTimeMillis() - startTime);
    }
}