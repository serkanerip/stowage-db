package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyValueLogStore {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueLogStore.class);

    public static final HeapData TOMBSTONE_MARKER = new HeapData(new byte[] {});

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
                    Thread.currentThread().interrupt();
                    logger.debug("Interrupted while waiting for active segment", e);
                    return;
                }
                var totalNumberOfEntries = inMemoryIndex.size();
                logger.info("Total number of entries: {}", totalNumberOfEntries);
            }
        });
        logger.info("Initialized KeyValueLogStore");
    }

    public void delete(HeapData key) {
        put(key, TOMBSTONE_MARKER);
    }

    public byte[] get(HeapData key) {
        var metadata = inMemoryIndex.get(key);
        if (metadata == null) {
            return null;
        }
        return segmentStore.getSegment(metadata.segmentId()).read(metadata);
    }

    public void put(HeapData key, HeapData value) {
        var dataEntry = new DataEntry(key, value);
        var metadata = activeSegment.write(dataEntry);
        var inMemoryMetadata = InMemoryIndex.EntryMetadata.fromPersistentEntryMetadata(
            activeSegment.getSegmentId(), metadata
        );
        inMemoryIndex.put(key, inMemoryMetadata);
        afterPut();
    }

    private void afterPut() {
        if (activeSegment.dataSize() >= DataSegment.SEGMENT_MAX_SIZE_IN_BYTES) {
            this.activeSegment = segmentStore.createEmptySegment();
        }
        var segmentsToDecommission = new ArrayList<String>();
        var segmentsToCompact = new ArrayList<String>();
        for (Map.Entry<String, SegmentStats> statsEntry : segmentStore.getSegmentStats()
            .entrySet()) {
            if (activeSegment.getSegmentId().equals(statsEntry.getKey())) {
                continue;
            }
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
            var inMemoryMetadata = inMemoryIndex.get(new HeapData(metadata.key()));
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
                    throw new UncheckedIOException(e);
                }
            }
        }
        segmentStore.replaceSegmentWithCompacted(segment, compactSegment, newSegmentStats);
        logger.info("Compacted segment {} in {} ms", segment, System.currentTimeMillis() - startTime);
    }

}
