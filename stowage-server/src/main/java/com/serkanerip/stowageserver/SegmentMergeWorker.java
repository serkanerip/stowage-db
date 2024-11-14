package com.serkanerip.stowageserver;

import static com.serkanerip.stowageserver.DataSegment.SEGMENT_MAX_SIZE_IN_BYTES;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentMergeWorker {

    private static final int MIN_SEGMENTS_FOR_COMPACTION = 2;
    private static final int MAX_SEGMENTS_FOR_COMPACTION = 10;
    private static final int COMPACTION_INTERVAL_MS = 30_000;
    private final Logger logger = LoggerFactory.getLogger(SegmentMergeWorker.class);
    private final Thread thread;
    private final StoreQueue storeQueue;
    private volatile boolean ongoingCompaction = false;
    private final ServerOptions options;
    private final KeyValueLogStore store;

    SegmentMergeWorker(StoreQueue storeQueue, ServerOptions options, KeyValueLogStore store) {
        this.storeQueue = storeQueue;
        this.options = options;
        this.store = store;
        this.thread = Thread.ofVirtual().unstarted(this::run);
    }

    void shutdown() {
        logger.info("Shutting down segment merge task");
        thread.interrupt();
    }

    void start() {
        thread.start();
    }

    private void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(COMPACTION_INTERVAL_MS);
                if (ongoingCompaction) {
                    logger.debug("Skipping compaction, ongoing compaction in progress!");
                    continue;
                }
            } catch (InterruptedException e) {
                break;
            }

            if (store.segments.size() < MIN_SEGMENTS_FOR_COMPACTION) {
                continue;
            }

            // Find candidate segments for size-tiered compaction
            List<DataSegment> candidates = findCompactionCandidates();
            if (candidates.isEmpty()) {
                continue;
            }

            logger.info("Compacting segments: {}",
                candidates.stream().map(DataSegment::getSegmentId).toList());

            DataSegment compactedSegment = compactSegments(candidates);

            ongoingCompaction = true;
            storeQueue.addToQueue(() -> {
                int insertionIndex = store.segments.indexOf(candidates.getFirst());
                store.segments.add(insertionIndex, compactedSegment);

                candidates.forEach(segment -> {
                    store.segments.remove(segment);
                    segment.decommission();
                });

                ongoingCompaction = false;
            });
        }
    }

    /**
     * Finds segments eligible for size-tiered compaction.
     */
    private List<DataSegment> findCompactionCandidates() {
        List<DataSegment> segments = store.segments.stream()
            .skip(1) // Skip first segment which is the active one
            .filter(segment -> segment.size() >= SEGMENT_MAX_SIZE_IN_BYTES && !segment.isMerged())
            .limit(MAX_SEGMENTS_FOR_COMPACTION)
            .toList();

        return segments.size() >= MIN_SEGMENTS_FOR_COMPACTION ? segments : Collections.emptyList();
    }

    /**
     * Compacts the given segments into a single new segment.
     */
    private DataSegment compactSegments(List<DataSegment> segments) {
        String newSegmentId = String.format("%s-merged", segments.getLast().getSegmentId());
        var newSegmentPath = options.dataRootPath().resolve("%s.data".formatted(newSegmentId));
        DataSegment newSegment = new DataSegment(newSegmentPath, false);

        Map<Data, DataSegment> latestEntries = new HashMap<>();

        // Merge entries from segments, preferring the latest segment's entries
        for (DataSegment segment : segments) {
            segment.index().forEach((key, index) -> {
                if (!latestEntries.containsKey(key)) {
                    latestEntries.put(key, segment);
                }
            });
        }

        // Write the latest entries to the new compacted segment
        latestEntries.forEach((key, segment) -> {
            var entry = segment.read(key);
            if (!entry.isDeleted()) {
                newSegment.write(entry);
            }
        });

        return newSegment;
    }
}
