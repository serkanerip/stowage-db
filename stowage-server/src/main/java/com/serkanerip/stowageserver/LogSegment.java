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

    public static final long SEGMENT_MAX_SIZE_IN_BYTES = 1L << 30;

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private final FileChannel fileChannel;

    final Path dataPath;

    private final Path indexPath;

    private final FileChannel indexChannel;

    private final String segmentId;

    private long dataSize;

    private long indexSize;

    public void decommission() {
        log.info("Decommissioning segment {}", segmentId);
        try {
            shutdown();
            Files.delete(dataPath);
            Files.delete(indexPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void shutdown() {
        log.info("Shutting down DataSegment");
        try {
            indexChannel.close();
            fileChannel.close();
        } catch (IOException e) {
            log.error("Error while shutting down data segment", e);
        }
    }

    public String getSegmentId() {
        return segmentId;
    }

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
            this.indexSize = indexChannel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileChannel getDataChannel() {
        return fileChannel;
    }

    public IndexChannelIterator newIndexIterator() {
        return new IndexChannelIterator(indexChannel);
    }

    public long dataSize() {
        return dataSize;
    }

    public byte[] read(InMemoryIndex.EntryMetadata entryMetadata) {
        var valSize = entryMetadata.valueSize();
        var valueOffset = entryMetadata.valueOffset();

        // Pre-conditions check
        if (valSize < 0) {
            throw new IllegalArgumentException("Value size cannot be negative: " + valSize);
        }
        if (valSize == 0) {
            return new byte[0];
        }

        var buff = ByteBuffer.allocateDirect(valSize);
        try {
            int bytesRead = 0;
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
                log.info("Had to do {} read to get full data", readCount);
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

    public EntryMetadata write(EntryRecord entryRecord) {
        try {
            var valueOffset = dataSize + 4 + entryRecord.getKey().size() + 4;
            dataSize += fileChannel.write(entryRecord.serialize(), dataSize);
            var metadata = new EntryMetadata(
                entryRecord.getKey().toByteArray(), entryRecord.getValue().size(), valueOffset
            );
            indexSize += indexChannel.write(metadata.serialize(), indexSize);
            return metadata;
        } catch (IOException e) {
            throw new DataEntryWriteFailedException(e);
        }
    }

    static class IndexChannelIterator implements Iterator<EntryMetadata> {

        private final ByteBuffer buffer;

        private IndexChannelIterator(FileChannel indexChannel) {
            try {
                this.buffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0L, indexChannel.size());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return buffer.hasRemaining();
        }

        @Override
        public EntryMetadata next() {
            return EntryMetadata.deserialize(buffer);
        }
    }
}
