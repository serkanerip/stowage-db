package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.serkanerip.stowagecommon.HeapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogStructuredStore {

    private final Map<String, LogSegment> segments = new HashMap<>();

    private final ServerOptions options;

    private final HashMap<String, SegmentStats> segmentStats = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(LogStructuredStore.class);

    public static final HeapData TOMBSTONE_MARKER = new HeapData(new byte[] {});

    private InMemoryIndex inMemoryIndex;

    private LogSegment activeSegment;

    private Thread monitorThread;

    public LogStructuredStore(ServerOptions options) {
        this.options = options;
    }

    public void shutdown() {
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        segments.values().forEach(LogSegment::shutdown);
    }

    public void init() {
        buildSegmentsFromFiles();
        deleteEmptySegments();
        this.inMemoryIndex = InMemoryIndex.fromLogSegments(segments);
        this.activeSegment = createEmptySegment();
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
                segmentStats.forEach((id, stats) -> logger.info("Segment {} stats {}", id, stats));
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
        return getSegment(metadata.segmentId()).read(metadata);
    }

    public void put(HeapData key, HeapData value) {
        var dataEntry = new EntryRecord(key, value);
        var metadata = activeSegment.write(dataEntry);
        var inMemoryMetadata = InMemoryIndex.EntryMetadata.fromPersistentEntryMetadata(
            activeSegment.getSegmentId(), metadata
        );
        var prevMetadata = inMemoryIndex.put(key, inMemoryMetadata);
        updateStats(key.size(), prevMetadata, inMemoryMetadata);
        afterPut();
    }

    private void afterPut() {
        if (activeSegment.dataSize() >= LogSegment.SEGMENT_MAX_SIZE_IN_BYTES) {
            this.activeSegment = createEmptySegment();
        }
        var segmentsToCompact = new ArrayList<String>();
        var segmentsToDecommission = new ArrayList<String>();
        for (Map.Entry<String, SegmentStats> statsEntry : getSegmentStats()
            .entrySet()) {
            if (activeSegment.getSegmentId().equals(statsEntry.getKey())) {
                continue;
            }
            if (statsEntry.getValue().obsoleteDataRatio() >= 0.30) {
                if (statsEntry.getValue().obsoleteDataRatio() == 1.0) {
                    segmentsToDecommission.add(statsEntry.getKey());
                } else {
                    segmentsToCompact.add(statsEntry.getKey());
                }

            }
        }
        segmentsToDecommission.forEach(this::decommission);
        segmentsToCompact.forEach(segmentId -> compactSegment(getSegment(segmentId)));
    }

    private void compactSegment(LogSegment segment) {
        var startTime = System.currentTimeMillis();
        logger.info("Compacting segment {}", segment.getSegmentId());
        var indexIterator = segment.newIndexIterator();
        var segmentDch = segment.getDataChannel();
        while (indexIterator.hasNext()) {
            var metadata = indexIterator.next();
            var keyHeapData = new HeapData(metadata.key());
            var inMemoryMetadata = inMemoryIndex.get(keyHeapData);
            if (Objects.equals(inMemoryMetadata.segmentId(), segment.getSegmentId())
                && inMemoryMetadata.valueOffset() == metadata.valueOffset()
            ) {
                try {
                    var newDataEntry = activeSegment.transferFrom(segmentDch, metadata);
                    inMemoryIndex.put(
                        keyHeapData,
                        InMemoryIndex.EntryMetadata.fromPersistentEntryMetadata(
                            activeSegment.getSegmentId(), newDataEntry
                        )
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        decommission(segment.getSegmentId());
        logger.info("Compacted segment {} in {} ms", segment.getSegmentId(),
            System.currentTimeMillis() - startTime);
    }

    void decommission(String segmentId) {
        logger.info("Decommissioning segment {}", segmentId);
        var segment = segments.remove(segmentId);
        segment.decommission();
        segmentStats.remove(segment.getSegmentId());
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

    HashMap<String, SegmentStats> getSegmentStats() {
        return segmentStats;
    }

    private void deleteEmptySegments() {
        var emptySegmentIds = new ArrayList<String>();
        segments.forEach((id, segment) -> {
            if (segment.dataSize() == 0L) {
                emptySegmentIds.add(id);
            }
        });
        emptySegmentIds.forEach(this::decommission);
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
                    .collect(Collectors.toMap(Utils::extractSegmentId, LogSegment::new))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Map<String, LogSegment> getSegments() {
        return segments;
    }

    LogSegment getSegment(String segmentId) {
        return segments.get(segmentId);
    }

    LogSegment createEmptySegment() {
        var id = System.currentTimeMillis() + "";
        var path = options.dataRootPath().resolve("%s.data".formatted(id));
        logger.info("Created new segment {}", id);
        var segment = new LogSegment(path);
        segments.put(id, segment);
        return segment;
    }

}
