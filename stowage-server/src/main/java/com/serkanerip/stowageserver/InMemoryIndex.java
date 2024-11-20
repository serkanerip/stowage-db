package com.serkanerip.stowageserver;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryIndex {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    private final HashMap<HeapData, MemoryEntryMetadata> index = new HashMap<>();

    private final Map<Long, SegmentStats> segmentStats;

    private InMemoryIndex(Map<Long, SegmentStats> segmentStats) {
        this.segmentStats = segmentStats;
    }

    static InMemoryIndex fromLogSegments(
        Map<Long, LogSegment> segments,
        Map<Long, SegmentStats> segmentStats
    ) {
        var startTime = System.currentTimeMillis();
        var instance = new InMemoryIndex(segmentStats);
        segments.keySet().stream().sorted(Comparator.naturalOrder())
            .forEach(segmentId -> {
                logger.info("Building memory index from segment {}", segmentId);
                var segment = segments.get(segmentId);
                var iterator = segment.newIndexIterator();
                while (iterator.hasNext()) {
                    var persistentMetadata = iterator.next();
                    instance.put(new HeapData(persistentMetadata.key()), new MemoryEntryMetadata(
                        segmentId, persistentMetadata.valueSize(), persistentMetadata.valueOffset()
                    ));
                }
            });
        logger.info("InMemoryIndex build took {} ms", System.currentTimeMillis() - startTime);
        return instance;
    }

    int size() {
        return index.size();
    }

    MemoryEntryMetadata get(HeapData key) {
        return index.get(key);
    }

    /**
     * Returns previous metadata for this key if any
     */
    void put(HeapData key, MemoryEntryMetadata metadata) {
        var previousMetadata = index.get(key);
        index.put(key, metadata);
        updateStats(key.size(), previousMetadata, metadata);
    }

    void updateStats(int keySize, MemoryEntryMetadata prevMetadata,
                     MemoryEntryMetadata newMetadata) {
        if (prevMetadata != null) {
            var id = prevMetadata.segmentId();
            segmentStats.compute(id, (segId, existingStats) -> {
                if (existingStats == null) {
                    existingStats = new SegmentStats();
                }
                existingStats.obsoleteKeyCount++;
                existingStats.obsoleteDataSize += prevMetadata.valueSize() + keySize;
                return existingStats;
            });
        }
        segmentStats.compute(newMetadata.segmentId(), (seg, stats) -> {
            if (stats == null) {
                stats = new SegmentStats();
            }
            stats.totalKeyCount++;
            stats.totalDataSize += newMetadata.valueSize() + keySize;
            return stats;
        });
    }

    record MemoryEntryMetadata(Long segmentId, int valueSize, long valueOffset) {
        static MemoryEntryMetadata fromPersistedEntryMetadata(
            Long segmentId, PersistentEntryMetadata persistentEntryMetadata
        ) {
            return new MemoryEntryMetadata(segmentId, persistentEntryMetadata.valueSize(),
                persistentEntryMetadata.valueOffset());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MemoryEntryMetadata that = (MemoryEntryMetadata) o;
            return valueSize == that.valueSize && valueOffset == that.valueOffset
                && Objects.equals(segmentId, that.segmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentId, valueSize, valueOffset);
        }
    }

}
