package com.serkanerip.stowageserver;

import java.util.Comparator;
import java.util.HashMap;

import com.serkanerip.stowagecommon.HeapData;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryIndex {

    record EntryMetadata(String segmentId, int valueSize, long valueOffset) {
        static EntryMetadata fromPersistentEntryMetadata(
            String segmentId, com.serkanerip.stowageserver.EntryMetadata entryMetadata) {
            return new EntryMetadata(segmentId, entryMetadata.valueSize(),
                entryMetadata.valueOffset());
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(InMemoryIndex.class);

    private final HashMap<HeapData, EntryMetadata> index = new HashMap<>();

    private InMemoryIndex() {
    }

    static InMemoryIndex emptyIndex() {
        return new InMemoryIndex();
    }

    static InMemoryIndex fromLogSegments(Map<String, LogSegment> segments) {
        var startTime = System.currentTimeMillis();
        var instance = new InMemoryIndex();
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
    EntryMetadata put(HeapData key, EntryMetadata metadata) {
        var previousMetadata = index.get(key);
        index.put(key, metadata);
        return previousMetadata;
    }

}
