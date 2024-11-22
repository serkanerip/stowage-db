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

    LogSegment(Path dataPath, State state, long flushThreshold) {
        this.flushThreshold = flushThreshold;
        this.state = state;
        this.dataPath = dataPath;
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

    void flushToDisk() throws IOException {
        if (unFlushedSize == 0) {
            return;
        }
        logger.debug("Flushing segment {} to disk", segmentId);
        fileChannel.force(true);
        indexChannel.force(true);
        unFlushedSize = 0L;
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
        indexChannel.write(newMetadata.serialize());
        if (unFlushedSize >= flushThreshold) {
            flushToDisk();
        }
        return newMetadata;
    }

    void decommission() {
        try {
            shutdown();
            Files.delete(dataPath);
            Files.delete(indexPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void shutdown() {
        logger.info("Shutting down DataSegment");
        try {
            indexChannel.force(true);
            fileChannel.force(true);
            indexChannel.close();
            fileChannel.close();
        } catch (IOException e) {
            logger.error("Error while shutting down data segment", e);
        }
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

        var buff = ByteBuffer.allocateDirect(valSize);
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

    PersistentEntryMetadata write(byte[] rawKey, byte[] rawValue, long sequenceNumber) {
        try {
            var entryRecord = new EntryRecord(rawKey, rawValue);
            var valueOffset =
                dataSize + Integer.BYTES + entryRecord.getKey().length + Integer.BYTES;
            var buffer = entryRecord.serialize();
            unFlushedSize += buffer.remaining();
            dataSize += fileChannel.write(buffer);
            var metadata = new PersistentEntryMetadata(
                entryRecord.getKey(), entryRecord.getValue().length, valueOffset, sequenceNumber
            );
            indexChannel.write(metadata.serialize());
            if (unFlushedSize >= flushThreshold) {
                flushToDisk();
            }
            return metadata;
        } catch (IOException e) {
            throw new DataEntryWriteFailedException(e);
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
            return PersistentEntryMetadata.deserialize(buffer);
        }
    }
}
