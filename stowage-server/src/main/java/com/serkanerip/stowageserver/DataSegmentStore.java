package com.serkanerip.stowageserver;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSegmentStore {

    private static final Logger logger = LoggerFactory.getLogger(DataSegmentStore.class);
    private final Map<String, DataSegment> segments = new HashMap<>();
    private final ServerOptions options;
    private final HashMap<String, SegmentStats> segmentStats = new HashMap<>();
    private final Thread monitoringThread;

    private DataSegmentStore(ServerOptions options) {
        this.options = options;
        this.monitoringThread = Thread.ofPlatform().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(15_000);
                } catch (InterruptedException e) {
                    break;
                }
                segmentStats.forEach((id, stats) -> logger.info("Segment {} stats {}", id, stats));
            }
            logger.debug("Segment stats monitoring thread interrupted, shutting down!");
        });
    }

    void shutdown() {
        monitoringThread.interrupt();
        segments.values().forEach(DataSegment::shutdown);
    }

    void decommission(String segmentId) {
        var segment = segments.remove(segmentId);
        segment.decommission();
        segmentStats.remove(segment.getSegmentId());
    }

    void updateStats(int keySize, InMemoryIndex.EntryMetadata prevMetadata, InMemoryIndex.EntryMetadata newMetadata) {
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

    HashMap<String, SegmentStats> getSegmentStats() {
        return segmentStats;
    }

    static DataSegmentStore create(ServerOptions options) {
        var store = new DataSegmentStore(options);
        store.buildSegmentsFromFiles();
        store.deleteEmptySegments();
        return store;
    }

    private void deleteEmptySegments() {
        var emptySegmentIds = new ArrayList<String>();
        segments.forEach((id, segment) -> {
            if (segment.size() == 0L) {
                emptySegmentIds.add(id);
            }
        });
        emptySegmentIds.forEach(id -> {
            var segment = segments.remove(id);
            segment.decommission();
            logger.info("Removed empty segment {}", id);
        });
    }

    private void buildSegmentsFromFiles() {
        var dataRootPath = options.dataRootPath();
        try {
            if (!dataRootPath.toFile().exists() && !dataRootPath.toFile().mkdirs()) {
                throw new RuntimeException();
            }
        } catch (Exception e) {
            logger.error("Failed to create data root path, shutting down!", e);
            System.exit(1);
        }
        try {
            segments.putAll(
                Files.list(dataRootPath).filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".data"))
                    .collect(Collectors.toMap(Utils::extractSegmentId, DataSegment::new))
            );
        } catch (IOException e) {
            logger.error("Failed to load data segments", e);
            System.exit(1);
            throw new RuntimeException(e);
        }
    }

    Map<String, DataSegment> getSegments() {
        return segments;
    }

    DataSegment getSegment(String segmentId) {
        return segments.get(segmentId);
    }

    DataSegment createEmptySegment() {
        var id = System.currentTimeMillis() + "";
        var path = options.dataRootPath().resolve("%s.data".formatted(id));
        logger.info("Created new segment {}", id);
        var segment = new DataSegment(path);
        segments.put(id, segment);
        return segment;
    }

    DataSegment createCompactedSegmentFor(DataSegment segment) {
        var id = segment.getSegmentId();
        var path = options.dataRootPath().resolve("%s-compact.data".formatted(id));
        logger.info("Created compacted segment for segment {}", id);
        return new DataSegment(path);
    }

    void replaceSegmentWithCompacted(DataSegment segment, DataSegment compactSegment,
                                     SegmentStats newSegmentStats) {
        var removedSegment = segments.remove(segment.getSegmentId());
        removedSegment.shutdown();
        segments.put(segment.getSegmentId(), compactSegment);
        segmentStats.put(segment.getSegmentId(), newSegmentStats);
        logger.info("Replaced segment {} with the compacted version!", segment.getSegmentId());
    }
}
