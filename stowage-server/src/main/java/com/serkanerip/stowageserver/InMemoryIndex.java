package com.serkanerip.stowageserver;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryIndex {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    private final Map<HeapData, MemoryEntryMetadata> index = new ConcurrentHashMap<>();

    private final Map<Long, SegmentStats> segmentStats;

    public InMemoryIndex(Map<Long, SegmentStats> segmentStats) {
        this.segmentStats = segmentStats;
    }

    public long rebuiltFromSegments(Map<Long, LogSegment> segments) {
        var startTime = System.currentTimeMillis();
        var maxSequenceNumberList = new CopyOnWriteArrayList<Long>();
        try (var es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            for (LogSegment segment : segments.values()) {
                es.submit(() -> {
                    var segmentMaxSequenceNumber = 0L;
                    var segmentId = segment.getId();
                    logger.info("Building memory index from segment {}", segmentId);
                    var iterator = segment.newIndexIterator();
                    while (iterator.hasNext()) {
                        var persistentMetadata = iterator.next();
                        var memoryMetadata = MemoryEntryMetadata.fromPersistedEntryMetadata(
                                segmentId, persistentMetadata);
                        segmentMaxSequenceNumber = Math.max(segmentMaxSequenceNumber,
                                persistentMetadata.sequenceNumber());
                        put(new HeapData(persistentMetadata.key()), memoryMetadata);
                    }
                    maxSequenceNumberList.add(segmentMaxSequenceNumber);
                });
            }
        }
        logger.info("InMemoryIndex rebuilt took {} ms", System.currentTimeMillis() - startTime);
        logger.info("index built from {} segments!", maxSequenceNumberList.size());
        return Collections.max(maxSequenceNumberList);
    }

    int size() {
        return index.size();
    }

    MemoryEntryMetadata get(HeapData key) {
        return index.get(key);
    }

    void put(HeapData key, MemoryEntryMetadata metadata) {
        putInternal(key, metadata);
    }

    private void putInternal(HeapData key, MemoryEntryMetadata metadata) {
        index.compute(key, (k, previousMetadata) -> {
            var isFresh = previousMetadata == null
                    || previousMetadata.sequenceNumber <= metadata.sequenceNumber;
            updateStats(key.size(), previousMetadata, metadata);
            return isFresh ? metadata : previousMetadata;
        });
    }

    private void incrementObsoleteData(MemoryEntryMetadata metadata, int keySize) {
        segmentStats.compute(metadata.segmentId(), (id, stats) -> {
            if (stats == null) {
                stats = new SegmentStats();
            }
            stats.obsoleteKeyCount++;
            stats.obsoleteDataSize += metadata.valueSize() + keySize;
            return stats;
        });
    }

    private void incrementNewData(MemoryEntryMetadata metadata, int keySize) {
        segmentStats.compute(metadata.segmentId(), (id, stats) -> {
            if (stats == null) {
                stats = new SegmentStats();
            }
            stats.totalKeyCount++;
            stats.totalDataSize += metadata.valueSize() + keySize;
            return stats;
        });
    }

    void updateStats(int keySize, MemoryEntryMetadata prevMetadata,
            MemoryEntryMetadata newMetadata) {
        boolean isPrevObsolete = prevMetadata != null && newMetadata.sequenceNumber >= prevMetadata.sequenceNumber;
        boolean isNewObsolete = prevMetadata != null && prevMetadata.sequenceNumber > newMetadata.sequenceNumber;

        if (isPrevObsolete) {
            incrementObsoleteData(prevMetadata, keySize);
        }
        if (isNewObsolete) {
            incrementObsoleteData(newMetadata, keySize);
        }
        if (prevMetadata == null || newMetadata.sequenceNumber >= prevMetadata.sequenceNumber) {
            incrementNewData(newMetadata, keySize);
        }
    }

    record MemoryEntryMetadata(long segmentId, int valueSize, long valueOffset,
            long sequenceNumber) {
        static MemoryEntryMetadata fromPersistedEntryMetadata(
                long segmentId, PersistentEntryMetadata persistentEntryMetadata) {
            return new MemoryEntryMetadata(
                    segmentId,
                    persistentEntryMetadata.valueSize(),
                    persistentEntryMetadata.valueOffset(),
                    persistentEntryMetadata.sequenceNumber());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MemoryEntryMetadata that = (MemoryEntryMetadata) o;
            return valueSize == that.valueSize && valueOffset == that.valueOffset
                    && sequenceNumber == that.sequenceNumber
                    && Objects.equals(segmentId, that.segmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentId, valueSize, valueOffset, sequenceNumber);
        }
    }

}
