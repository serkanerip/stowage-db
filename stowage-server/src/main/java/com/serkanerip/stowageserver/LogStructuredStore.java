package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
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

    private final ReentrantLock writeLock = new ReentrantLock(true);

    private Thread monitorThread;

    private final LogSegmentCompacter compacter;

    public LogStructuredStore(ServerOptions options) {
        this.options = options;
        this.compacter = new LogSegmentCompacter(this);
    }

    public void shutdown() {
        compacter.shutdown();
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        segments.values().forEach(LogSegment::shutdown);
    }

    public void init() {
        buildSegmentsFromFiles();
        deleteEmptySegments();
        this.inMemoryIndex = InMemoryIndex.fromLogSegments(segments, segmentStats);
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

    ReentrantLock getWriteLock() {
        return writeLock;
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
        writeLock.lock();
        try {
            var dataEntry = new EntryRecord(key, value);
            var metadata = activeSegment.write(dataEntry);
            var inMemoryMetadata = InMemoryIndex.MemoryEntryMetadata.fromPersistedEntryMetadata(
                activeSegment.getId(), metadata
            );
            inMemoryIndex.put(key, inMemoryMetadata);
            afterPut();
        } finally {
            writeLock.unlock();
        }
    }

    private void afterPut() {
        if (activeSegment.getDataSize() >= LogSegment.SEGMENT_MAX_SIZE_IN_BYTES) {
            this.activeSegment = createEmptySegment();
        }
        var segmentsToDecommission = new ArrayList<String>();
        for (Map.Entry<String, SegmentStats> statsEntry : getSegmentStats()
            .entrySet()) {
            if (activeSegment.getId().equals(statsEntry.getKey())) {
                continue;
            }
            if (statsEntry.getValue().obsoleteDataRatio() >= 0.30) {
                if (statsEntry.getValue().obsoleteDataRatio() == 1.0) {
                    segmentsToDecommission.add(statsEntry.getKey());
                } else {
                    compacter.offer(statsEntry.getKey());
                }

            }
        }
        segmentsToDecommission.forEach(this::decommission);
    }

    InMemoryIndex getInMemoryIndex() {
        return inMemoryIndex;
    }

    LogSegment getActiveSegment() {
        return activeSegment;
    }

    void decommission(String segmentId) {
        logger.info("Decommissioning segment {}", segmentId);
        var segment = segments.remove(segmentId);
        segment.decommission();
        segmentStats.remove(segment.getId());
    }

    HashMap<String, SegmentStats> getSegmentStats() {
        return segmentStats;
    }

    private void deleteEmptySegments() {
        var emptySegmentIds = new ArrayList<String>();
        segments.forEach((id, segment) -> {
            if (segment.getDataSize() == 0L) {
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

    LogSegment getSegment(String segmentId) {
        return segments.get(segmentId);
    }

    LogSegment createEmptySegment() {
        var id = System.currentTimeMillis() + "";
        var path = options.dataRootPath().resolve("%s.data".formatted(id));
        logger.info("Created new segment {}", id);
        var segment = new LogSegment(path);
        segments.put(id, segment);
        segmentStats.put(id, new SegmentStats());
        return segment;
    }

}
