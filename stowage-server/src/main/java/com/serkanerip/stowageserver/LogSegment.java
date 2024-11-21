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

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final FileChannel fileChannel;

    private final Path dataPath;

    private final Path indexPath;

    private final FileChannel indexChannel;

    private final long segmentId;

    private long dataSize;

    public LogSegment(Path dataPath) {
        try {
            this.dataPath = dataPath;
            this.segmentId = Utils.extractSegmentId(dataPath);
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

    PersistentEntryMetadata transferFrom(
        FileChannel sourceChannel, PersistentEntryMetadata metadata
    ) throws IOException {
        var readPos =
            metadata.valueOffset() - Integer.BYTES - metadata.key().length - Integer.BYTES;
        var readCount =
            Integer.BYTES + metadata.key().length + Integer.BYTES + metadata.valueSize();
        var newValOffset = dataSize + Integer.BYTES + metadata.key().length + Integer.BYTES;
        dataSize += sourceChannel.transferTo(readPos, readCount, fileChannel);
        var newMetadata = new PersistentEntryMetadata(
            metadata.key(), metadata.valueSize(), newValOffset, metadata.sequenceNumber()
        );
        indexChannel.write(newMetadata.serialize());
        return newMetadata;

    }

    public void decommission() {
        try {
            shutdown();
            Files.delete(dataPath);
            Files.delete(indexPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void shutdown() {
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

    public long getId() {
        return segmentId;
    }

    public FileChannel getDataChannel() {
        return fileChannel;
    }

    long getDataSize() {
        return dataSize;
    }

    public IndexChannelIterator newIndexIterator() {
        return new IndexChannelIterator(indexChannel);
    }

    public byte[] read(InMemoryIndex.MemoryEntryMetadata memoryEntryMetadata) {
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

    public PersistentEntryMetadata write(byte[] rawKey, byte[] rawValue, long sequenceNumber) {
        try {
            var entryRecord = new EntryRecord(rawKey, rawValue);
            var valueOffset =
                dataSize + Integer.BYTES + entryRecord.getKey().length + Integer.BYTES;
            dataSize += fileChannel.write(entryRecord.serialize());
            var metadata = new PersistentEntryMetadata(
                entryRecord.getKey(), entryRecord.getValue().length, valueOffset, sequenceNumber
            );
            indexChannel.write(metadata.serialize());
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
