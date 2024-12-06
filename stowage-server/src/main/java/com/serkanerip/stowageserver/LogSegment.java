package com.serkanerip.stowageserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.serkanerip.stowageserver.exception.DataEntryReadFailedException;
import com.serkanerip.stowageserver.exception.DataEntryWriteFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogSegment {

    enum State {
        ACTIVE,      // Segment receiving writes
        READ_ONLY,   // Segment used only for reads
        COMPACTING,  // Segment in compaction queue or being processed
        STALE        // Completely obsolete, ready for decommissioning
    }

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final FileChannel fileChannel;

    private final Path dataPath;

    private final Path indexPath;

    private final FileChannel indexChannel;

    private final long segmentId;

    private long dataSize;

    private State state;

    private long unFlushedSize = 0L;

    private final long flushThreshold;

    private volatile boolean decommissioned = false;

    // Concurrent reads are allowed hence ThreadLocal is appropriate
    private final ThreadLocal<ByteBuffer> readBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(1024 * 1024) // 1Mib
    );

    LogSegment(Path dataPath, State state, long flushThreshold) {
        this.dataPath = dataPath;
        this.state = state;
        this.flushThreshold = flushThreshold;
        this.segmentId = Utils.extractSegmentId(dataPath);
        try {
            this.indexPath = dataPath.getParent().resolve("%s.index".formatted(segmentId));
            this.fileChannel = FileChannel.open(dataPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.dataSize = fileChannel.size();
            fileChannel.position(dataSize);
            indexChannel.position(indexChannel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void setState(State state) {
        this.state = state;
    }

    State getState() {
        return state;
    }

    PersistentEntryMetadata transferFrom(
        FileChannel sourceChannel, PersistentEntryMetadata metadata
    ) throws IOException {
        var readPos =
            metadata.valueOffset() - Integer.BYTES - metadata.key().length - Integer.BYTES;
        var readCount =
            Integer.BYTES + metadata.key().length + Integer.BYTES + metadata.valueSize();
        var newValOffset = dataSize + Integer.BYTES + metadata.key().length + Integer.BYTES;
        unFlushedSize += readCount;
        dataSize += sourceChannel.transferTo(readPos, readCount, fileChannel);
        var newMetadata = new PersistentEntryMetadata(
            metadata.key(), metadata.valueSize(), newValOffset, metadata.sequenceNumber()
        );
        writeToIndex(newMetadata);
        if (unFlushedSize >= flushThreshold) {
            flushToDisk();
        }
        return newMetadata;
    }

    void decommission() {
        try {
            readBuffer.remove();
            indexChannel.close();
            fileChannel.close();
            Files.delete(dataPath);
            Files.delete(indexPath);
            decommissioned = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void shutdown() {
        if (decommissioned) {
            logger.warn("Cannot shutdown decommissioned log segment {}!", segmentId);
            return;
        }
        logger.info("Shutting down log segment {}", segmentId);
        readBuffer.remove();
        shutdownChannel(fileChannel, dataPath);
        shutdownChannel(indexChannel, indexPath);
    }

    long getId() {
        return segmentId;
    }

    FileChannel getDataChannel() {
        return fileChannel;
    }

    long getDataSize() {
        return dataSize;
    }

    IndexChannelIterator newIndexIterator() {
        return new IndexChannelIterator(indexChannel);
    }

    // Thread safe
    byte[] read(InMemoryIndex.MemoryEntryMetadata memoryEntryMetadata) {
        var valSize = memoryEntryMetadata.valueSize();
        var valueOffset = memoryEntryMetadata.valueOffset();

        // Pre-conditions check
        if (valSize < 0) {
            throw new IllegalArgumentException("Value size cannot be negative: " + valSize);
        }
        if (valSize == 0) {
            return new byte[0];
        }

        var localBuff = readBuffer.get();
        localBuff.clear();
        ByteBuffer buff = localBuff.capacity() > valSize ? localBuff : ByteBuffer.allocate(valSize);
        try {
            int bytesRead;
            int totalBytesRead = 0;

            // Handle partial reads
            int readCount = 0;
            while (totalBytesRead < valSize) {
                bytesRead = fileChannel.read(buff, valueOffset + totalBytesRead);

                if (bytesRead == -1) {  // EOF reached
                    throw new DataEntryReadFailedException(
                        "Unexpected EOF: expectedSize=%d, got=%d".formatted(
                            valSize, bytesRead
                        ));
                }

                totalBytesRead += bytesRead;
                readCount++;
            }
            if (readCount > 1) {
                logger.info("Had to do {} read to get full data", readCount);
            }

            buff.flip();
            var readValue = new byte[valSize];
            buff.get(readValue);
            return readValue;
        } catch (IOException e) {
            throw new DataEntryReadFailedException("Failed to read data entry", e);
        } catch (BufferUnderflowException e) {
            throw new DataEntryReadFailedException("Buffer underflow while reading data entry", e);
        }
    }

    // Not thread safe
    PersistentEntryMetadata write(byte[] rawKey, byte[] rawValue, long sequenceNumber) {
        try {
            var entryRecord = new EntryRecord(rawKey, rawValue);
            var valueOffset =
                dataSize + Integer.BYTES + entryRecord.getKey().length + Integer.BYTES;
            var buffer = entryRecord.serialize();
            while (buffer.hasRemaining()) {
                var writtenBytes = fileChannel.write(buffer);
                unFlushedSize += writtenBytes;
                dataSize += writtenBytes;
            }
            var metadata = new PersistentEntryMetadata(
                entryRecord.getKey(), entryRecord.getValue().length, valueOffset, sequenceNumber
            );
            writeToIndex(metadata);
            if (unFlushedSize >= flushThreshold) {
                flushToDisk();
            }
            return metadata;
        } catch (IOException e) {
            throw new DataEntryWriteFailedException(e);
        }
    }

    private void shutdownChannel(FileChannel ch, Path path) {
        try {
            // Check if file is removed, if yes create new one and transfer data from existing fd
            if (!Files.exists(path)) {
                logger.warn("Path {} does not exist, creating new one!", path);
                var newCh = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
                ch.transferTo(0, ch.size(), newCh);
                newCh.force(true);
                newCh.close();
                ch.close();
                logger.warn("New file created for path {} and all data transferred from channel!", path);
                return;
            }
            ch.force(true);
            ch.close();
        } catch (IOException e) {
            logger.error("Error while shutting channel of {}", path, e);
        }
    }

    private void flushToDisk() throws IOException {
        if (unFlushedSize == 0) {
            return;
        }
        logger.debug("Flushing segment {} to disk", segmentId);
        fileChannel.force(true);
        indexChannel.force(true);
        unFlushedSize = 0L;
    }

    private void writeToIndex(PersistentEntryMetadata metadata) throws IOException {
        var buffer = metadata.serialize();
        while (buffer.hasRemaining()) {
            indexChannel.write(buffer);
        }
    }

    static class IndexChannelIterator implements Iterator<PersistentEntryMetadata> {

        private final ByteBuffer buffer;

        private IndexChannelIterator(FileChannel indexChannel) {
            try {
                this.buffer =
                    indexChannel.map(FileChannel.MapMode.READ_ONLY, 0L, indexChannel.size());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public PersistentEntryMetadata next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return PersistentEntryMetadata.deserialize(buffer);
        }
    }
}
