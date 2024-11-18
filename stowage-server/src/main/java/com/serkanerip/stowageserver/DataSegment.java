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

class DataSegment {

    public static final long SEGMENT_MAX_SIZE_IN_BYTES = 1L << 30;

    private static final Logger log = LoggerFactory.getLogger(DataSegment.class);

    private final FileChannel fileChannel;

    final Path dataPath;

    private final Path indexPath;

    private final FileChannel indexChannel;

    private final String segmentId;

    private long dataSize = 0L;

    private long indexSize = 0L;

    public FileChannel getIndexChannel() {
        return indexChannel;
    }

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

    public DataSegment(Path dataPath) {
        try {
            this.dataPath = dataPath;
            this.segmentId = Utils.extractSegmentId(dataPath);
            this.indexPath = dataPath.getParent().resolve("%s.index".formatted(segmentId));
            this.fileChannel = FileChannel.open(dataPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
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
        var buff = ByteBuffer.allocateDirect(valSize);
        try {
            fileChannel.read(buff, valueOffset);
            buff.flip();
            if (buff.remaining() != valSize) {
                throw new DataEntryReadFailedException(
                    "Failed to read data entry expectedSize=%d, got=%d".formatted(
                        valSize, buff.remaining()
                    ));
            }
            var readValue = new byte[valSize];
            buff.get(readValue);
            return readValue;
        } catch (IOException | BufferUnderflowException e) {
            throw new DataEntryReadFailedException(e);
        }
    }

    public EntryMetadata write(DataEntry dataEntry) {
        try {
            var valueOffset = dataSize + 4 + dataEntry.getKey().size() + 4;
            dataSize += fileChannel.write(dataEntry.serialize(), dataSize);
            var metadata = new EntryMetadata(
                dataEntry.getKey().toByteArray(), dataEntry.getValue().size(), valueOffset
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
