package com.serkanerip.stowageserver;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KeyValueLogStore {

    public static final byte[] TOMBSTONE_MARKER = new byte[] {};

    private static final Logger logger = LoggerFactory.getLogger(KeyValueLogStore.class);

    final List<DataSegment> segments;

    private Thread monitorThread;

    private final ServerOptions options;

    public KeyValueLogStore(ServerOptions serverOptions, List<DataSegment> segments) {
        this.options = serverOptions;
        this.segments = segments;
    }

    public void shutdown() {
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        segments.forEach(DataSegment::shutdown);
    }

    public void init() {
        if (segments.isEmpty()) {
            segments.add(createNewSegment());
        }
        monitorThread = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10_000L);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for active segment", e);
                    return;
                }
                var totalNumberOfEntries =
                    segments.stream().map(DataSegment::entryCount).reduce(0L, Long::sum);
                logger.info("Total number of entries: {}", totalNumberOfEntries);
            }
        });
        logger.info("Initialized KeyValueLogStore");
    }

    public void delete(byte[] key) {
        put(key, new byte[] {});
    }

    public byte[] get(byte[] key) {
        var keyData = new Data(key);
        var dataEntry = segments.stream().filter(s -> s.hasKey(keyData))
            .findFirst().map(s -> s.read(keyData)).orElse(null);
        if (dataEntry == null || dataEntry.isDeleted()) {
            return null;
        }
        return dataEntry.getValue().getData();
    }

    public void put(byte[] key, byte[] value) {
        var dataEntry = new DataEntry(new Data(key), new Data(value));
        segments.getFirst().write(dataEntry);
        afterPut();
    }

    private void afterPut() {
        if (segments.getFirst().size() >= DataSegment.SEGMENT_MAX_SIZE_IN_BYTES) {
            var newSegment = createNewSegment();
            segments.addFirst(newSegment);
        }
    }

    private DataSegment createNewSegment() {
        var id = System.currentTimeMillis() + "";
        var path = options.dataRootPath().resolve("%s.data".formatted(id));
        return new DataSegment(path, false);
    }

}
