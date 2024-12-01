package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.serkanerip.stowageserver.exception.DataPathAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class StowageDB {

    private static final Logger logger = LoggerFactory.getLogger(StowageDB.class);

    private static final byte[] TOMBSTONE_MARKER = new byte[] {};

    private final Map<Long, SegmentStats> segmentStats = new ConcurrentHashMap<>();

    private final Map<Long, LogSegment> segments = new ConcurrentHashMap<>();

    private final ServerOptions options;

    private final InMemoryIndex inMemoryIndex;

    private final ReentrantLock writeLock = new ReentrantLock(true);

    private final LogSegmentCompacter compacter;

    private final AtomicLong nextSegmentId = new AtomicLong(System.currentTimeMillis());

    private final AtomicLong nextSequenceNumber = new AtomicLong(0L);

    private LogSegment activeSegment;

    private Thread monitorThread;

    public StowageDB(ServerOptions options) {
        this.options = Objects.requireNonNull(options);
        this.ensureHasReadWritePermissions(options.dataRootPath());
        this.compacter = new LogSegmentCompacter(this);
        this.inMemoryIndex = new InMemoryIndex(segmentStats);
        this.start();
    }

    /**
     * Starts the database service.
     * <p>
     * This method initializes the necessary in-memory index, active log segment,
     * and monitoring thread. It must be called before any other operations
     * on the database. Calling this method on an already started database
     * will result in an exception.
     * </p>
     *
     * @throws IllegalStateException if the database is already started.
     */
    private void start() {
        buildSegmentsFromFiles();
        deleteEmptySegments();
        nextSequenceNumber.set(inMemoryIndex.rebuiltFromSegments(segments) + 1);
        this.activeSegment = createEmptySegment(LogSegment.State.ACTIVE);
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
        logger.info("Database is initialized");
    }

    /**
     * Shuts down the database service.
     * <p>
     * Stops the compaction process, interrupts the monitoring thread,
     * and safely shuts down all log segments. This method should be called
     * before terminating the application to ensure a clean shutdown.
     * </p>
     */
    public void shutdown() {
        compacter.shutdown();
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        segments.values().forEach(LogSegment::shutdown);
    }


    /**
     * Retrieves the value associated with the specified key.
     * <p>
     * Returns the value corresponding to the given key or {@code null} if the key does not exist.
     * </p>
     *
     * @param rawKey the key of the entry to retrieve.
     * @return the value associated with the key, or {@code null} if not found.
     */
    public byte[] get(byte[] rawKey) {
        return get(rawKey, 0);
    }

    private byte[] get(byte[] rawKey, int attempt) {
        var metadata = inMemoryIndex.get(new HeapData(rawKey));
        if (metadata == null || metadata.valueSize() == 0) {
            return null;
        }
        if (attempt > 3) {
            logger.error("Segment {} not found after 3 attempt!", metadata.segmentId());
            return null;
        }
        var segment = getSegment(metadata.segmentId());
        if (segment == null) {
            logger.warn("Segment {} not found", metadata.segmentId());
            return get(rawKey, attempt + 1);
        }
        return segment.read(metadata);
    }

    public byte[] get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Stores a key-value pair in the database.
     * <p>
     * Adds a new entry or updates an existing entry with the given key and value.
     * </p>
     *
     * @param rawKey the key of the entry.
     * @param rawValue the value to associate with the key.
     */
    public void put(byte[] rawKey, byte[] rawValue) {
        if (rawKey == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }
        if (rawValue == null) {
            throw new IllegalArgumentException("Value cannot be null!");
        }
        writeLock.lock();
        try {
            var metadata = activeSegment.write(rawKey, rawValue, nextSequenceNumber.incrementAndGet());
            var inMemoryMetadata = InMemoryIndex.MemoryEntryMetadata.fromPersistedEntryMetadata(
                activeSegment.getId(), metadata
            );
            inMemoryIndex.put(new HeapData(rawKey), inMemoryMetadata);
            afterPut();
        } finally {
            writeLock.unlock();
        }
    }

    public void put(String key, byte[] rawValue) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        put(key.getBytes(StandardCharsets.UTF_8), rawValue);
    }

    /**
     * Deletes an entry from the database.
     * <p>
     * Marks the entry associated with the given key as deleted. The actual deletion
     * is deferred and handled by the compaction process.
     * </p>
     *
     * @param rawKey the key of the entry to delete.
     */
    public void delete(byte[] rawKey) {
        put(rawKey, TOMBSTONE_MARKER);
    }

    public void delete(String key) {
        put(key.getBytes(StandardCharsets.UTF_8), TOMBSTONE_MARKER);
    }

    private void afterPut() {
        if (activeSegment.getDataSize() >= options.maxFileSize()) {
            this.activeSegment.setState(LogSegment.State.READ_ONLY);
            this.activeSegment = createEmptySegment(LogSegment.State.ACTIVE);
        }
        var segmentsToDecommission = new ArrayList<Long>();
        for (Map.Entry<Long, SegmentStats> statsEntry : segmentStats.entrySet()) {
            var id = statsEntry.getKey();
            var segment = getSegment(id);
            if (segment == null || LogSegment.State.READ_ONLY != segment.getState()) {
                continue;
            }
            var obsoleteDataRatio = statsEntry.getValue().obsoleteDataRatio();
            if (obsoleteDataRatio >= options.compactionThreshold()) {
                if (obsoleteDataRatio == 1.0) {
                    segment.setState(LogSegment.State.STALE);
                    segmentsToDecommission.add(id);
                } else {
                    if (compacter.offer(statsEntry.getKey())) {
                        segment.setState(LogSegment.State.COMPACTING);
                    } else {
                        logger.warn("Failed to offer segment {} for compaction!", id);
                    }
                }
            }
        }
        segmentsToDecommission.forEach(id -> {
            logger.info("Decommissioning completely stale segment {}", id);
            decommission(id);
        });
    }

    InMemoryIndex getInMemoryIndex() {
        return inMemoryIndex;
    }

    void decommission(Long segmentId) {
        var segment = segments.remove(segmentId);
        segmentStats.remove(segmentId);
        if (segment == null) {
            logger.warn("Segment {} cannot be decommissioned because it is not found!", segmentId);
            return;
        }
        segment.decommission();
    }

    Map<Long, SegmentStats> getSegmentStats() {
        // Return a defensive copy of the segmentStats map
        return new HashMap<>(segmentStats);
    }

    private void ensureHasReadWritePermissions(Path path) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new DataPathAccessException(
                    String.format("Failed to create the file at '%s'.", path), e
                );
            }
        }

        if (!Files.isReadable(path) || !Files.isWritable(path)) {
            throw new DataPathAccessException(path);
        }
    }

    private void deleteEmptySegments() {
        var emptySegmentIds = new ArrayList<Long>();
        segments.forEach((id, segment) -> {
            if (segment.getDataSize() == 0L) {
                emptySegmentIds.add(id);
            }
        });
        emptySegmentIds.forEach(this::decommission);
    }

    private void buildSegmentsFromFiles() {
        var dataRootPath = options.dataRootPath();
        try (var files = Files.list(dataRootPath)) {
            files.filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".data"))
                .forEach(path -> {
                    var id = Utils.extractSegmentId(path);
                   segments.put(id, new LogSegment(
                       path, LogSegment.State.READ_ONLY, options.flushDataSize()
                   ));
                   if (id > nextSegmentId.get()) {
                       nextSegmentId.set(id);
                   }
                });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    LogSegment getSegment(Long segmentId) {
        return segments.get(segmentId);
    }

    LogSegment createEmptySegment(LogSegment.State state) {
        var id = nextSegmentId.incrementAndGet();
        var path = options.dataRootPath().resolve("%s.data".formatted(id));
        logger.info("Created new segment {}", id);
        var segment = new LogSegment(path, state, options.flushDataSize());
        segments.put(id, segment);
        segmentStats.put(id, new SegmentStats());
        return segment;
    }

}
