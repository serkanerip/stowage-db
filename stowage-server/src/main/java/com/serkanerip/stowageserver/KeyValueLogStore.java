package com.serkanerip.stowageserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyValueLogStore {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueLogStore.class);

    public static final byte[] TOMBSTONE_MARKER = new byte[] {};

    private final InMemoryIndex inMemoryIndex;

    private final DataSegmentStore segmentStore;

    private DataSegment activeSegment;

    private Thread monitorThread;

    public KeyValueLogStore(
        InMemoryIndex inMemoryIndex, DataSegmentStore segmentStore
    ) {
        this.inMemoryIndex = inMemoryIndex;
        this.segmentStore = segmentStore;
    }

    public void shutdown() {
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        segmentStore.shutdown();
    }

    public void init() {
        this.activeSegment = segmentStore.createEmptySegment();
        this.monitorThread = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10_000L);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for active segment", e);
                    return;
                }
                var totalNumberOfEntries = inMemoryIndex.size();
                logger.info("Total number of entries: {}", totalNumberOfEntries);
            }
        });
        logger.info("Initialized KeyValueLogStore");
    }

    public void delete(byte[] key) {
        put(key, TOMBSTONE_MARKER);
    }

    public byte[] get(byte[] key) {
        var keyData = new Data(key);
        var metadata = inMemoryIndex.get(keyData);
        if (metadata == null) {
            return null;
        }
        return segmentStore.getSegment(metadata.segmentId()).read(metadata);
    }

    public void put(byte[] key, byte[] value) {
        var keyData = new Data(key);
        var dataEntry = new DataEntry(keyData, new Data(value));
        var metadata = activeSegment.write(dataEntry);
        var inMemoryMetadata = InMemoryIndex.EntryMetadata.fromPersistentEntryMetadata(
            activeSegment.getSegmentId(), metadata
        );
        inMemoryIndex.put(keyData, inMemoryMetadata);
        afterPut();
    }

    private void afterPut() {
        if (activeSegment.size() >= DataSegment.SEGMENT_MAX_SIZE_IN_BYTES) {
            this.activeSegment = segmentStore.createEmptySegment();
        }
        var segmentsToDecommission = new ArrayList<String>();
        var segmentsToCompact = new ArrayList<String>();
        for (Map.Entry<String, SegmentStats> statsEntry : segmentStore.getSegmentStats()
            .entrySet()) {
            if (statsEntry.getValue().obsoleteDataRatio() >= 0.6) {
                if (statsEntry.getValue().obsoleteDataRatio() == 1.0) {
                    segmentsToDecommission.add(statsEntry.getKey());
                } else {
                    segmentsToCompact.add(statsEntry.getKey());
                }
            }
        }
        segmentsToDecommission.forEach(segmentStore::decommission);
        segmentsToCompact.forEach(id -> compactSegment(segmentStore.getSegment(id)));
    }

    private void compactSegment(DataSegment segment) {
        var startTime = System.currentTimeMillis();
        logger.info("Compacting segment {}", segment);
        var compactSegment = segmentStore.createCompactedSegmentFor(segment);
        var indexIterator = segment.newIndexIterator();
        var segmentDch = segment.getDataChannel();
        var compactSegmentDch = compactSegment.getDataChannel();
        var newSegmentStats = new SegmentStats();
        while (indexIterator.hasNext()) {
            var metadata = indexIterator.next();
            var inMemoryMetadata = inMemoryIndex.get(new Data(metadata.key()));
            if (Objects.equals(inMemoryMetadata.segmentId(), segment.getSegmentId())
                && inMemoryMetadata.valueOffset() == metadata.valueOffset()
            ) {
                newSegmentStats.totalKeyCount++;
                newSegmentStats.totalDataSize += metadata.valueSize() + metadata.key().length;
                var position = metadata.valueOffset() - Integer.BYTES - metadata.key().length - Integer.BYTES;
                var count = Integer.BYTES + metadata.key().length + Integer.BYTES + metadata.valueSize();
                try {
                    segmentDch.transferTo(position, count, compactSegmentDch);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        segmentStore.replaceSegmentWithCompacted(segment, compactSegment, newSegmentStats);
        logger.info("Compacted segment {} in {} ms", segment, System.currentTimeMillis() - startTime);
    }

}
