package com.serkanerip.stowageserver;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryIndex {

    record EntryMetadata(String segmentId, int valueSize, long valueOffset) {
        static EntryMetadata fromPersistentEntryMetadata(
            String segmentId, com.serkanerip.stowageserver.EntryMetadata entryMetadata) {
            return new EntryMetadata(segmentId, entryMetadata.valueSize(),
                entryMetadata.valueOffset());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EntryMetadata that = (EntryMetadata) o;
            return valueSize == that.valueSize && valueOffset == that.valueOffset
                && Objects.equals(segmentId, that.segmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentId, valueSize, valueOffset);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    private final HashMap<HeapData, EntryMetadata> index = new HashMap<>();

    private final Map<String, SegmentStats> segmentStats;

    private InMemoryIndex(Map<String, SegmentStats> segmentStats) {
        this.segmentStats = segmentStats;
    }

    static InMemoryIndex fromLogSegments(
        Map<String, LogSegment> segments,
        Map<String, SegmentStats> segmentStats
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
                    instance.put(new HeapData(persistentMetadata.key()), new EntryMetadata(
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

    EntryMetadata get(HeapData key) {
        return index.get(key);
    }

    /**
     * Returns previous metadata for this key if any
     */
    void put(HeapData key, EntryMetadata metadata) {
        var previousMetadata = index.get(key);
        index.put(key, metadata);
        updateStats(key.size(), previousMetadata, metadata);
    }

    void updateStats(int keySize, InMemoryIndex.EntryMetadata prevMetadata,
                     InMemoryIndex.EntryMetadata newMetadata) {
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

}
