package com.serkanerip.stowageserver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import com.serkanerip.stowageserver.exception.DataEntryReadFailedException;
import com.serkanerip.stowageserver.exception.DataEntryWriteFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataSegment {

    public static final long SEGMENT_MAX_SIZE_IN_BYTES = 2L * 1024 * 1024;

    private static final Logger log = LoggerFactory.getLogger(DataSegment.class);

    private final Map<Data, SegmentIndex> index = new HashMap<>();

    private final FileChannel fileChannel;

    private final Path dataPath;

    private final Path indexPath;

    private final String segmentId;

    private final boolean merged;

    public void decommission() {
        try {
            fileChannel.close();
            Files.delete(dataPath);
            Files.delete(indexPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        log.info("Shutting down DataSegment");
        try (ObjectOutputStream oos = new ObjectOutputStream(
            new FileOutputStream(indexPath.toFile()))) {
            fileChannel.close();
            oos.writeObject(index);
        } catch (IOException e) {
            log.error("Error while shutting down data segment", e);
        }
    }

    Map<Data, SegmentIndex> index() {
        return index;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public boolean isMerged() {
        return merged;
    }

    public DataSegment(Path dataPath, boolean reloadIndex) {
        try {
            this.merged = dataPath.getFileName().toString().contains("-merged");
            this.dataPath = dataPath;
            this.segmentId = dataPath.getFileName().toString().split(".data")[0];
            this.indexPath = dataPath.getParent().resolve("%s.index".formatted(segmentId));
            this.fileChannel = FileChannel.open(dataPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            if (reloadIndex) {
                ObjectInputStream ois =
                    new ObjectInputStream(new FileInputStream(indexPath.toFile()));
                Map<Data, SegmentIndex> loadedIndex = (Map<Data, SegmentIndex>) ois.readObject();
                index.clear();
                index.putAll(loadedIndex);
            }
        } catch (IOException e) {
            log.error("Failed to open file channel", e);
            System.exit(1);
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            log.error("Failed to reload index", e);
            System.exit(1);
            throw new RuntimeException(e);
        }
    }

    public long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long entryCount() {
        return index.size();
    }

    public boolean hasKey(Data key) {
        return index.containsKey(key);
    }

    public DataEntry read(Data key) {
        var i = index.get(key);
        if (i == null) {
            return null;
        }
        var buff = ByteBuffer.allocateDirect(i.getValueSize());
        var valueOffset = i.getValueOffset();
        try {
            fileChannel.read(buff, valueOffset);
            buff.flip();
            if (buff.remaining() != i.getValueSize()) {
                throw new DataEntryReadFailedException(
                    "Failed to read data entry expectedSize=%d, got=%d".formatted(
                        i.getValueSize(), buff.remaining()
                    ));
            }
            var readValue = new byte[i.getValueSize()];
            buff.get(readValue);
            return new DataEntry(key, new Data(readValue));
        } catch (IOException | BufferUnderflowException e) {
            throw new DataEntryReadFailedException(e);
        }
    }

    public void write(DataEntry dataEntry) {
        try {
            var valueOffset = fileChannel.size() + 4 + dataEntry.getKey().size() + 4;
            dataEntry.persistTo(fileChannel);
            index.put(dataEntry.getKey(),
                new SegmentIndex(dataEntry.getValue().size(), valueOffset));
        } catch (IOException e) {
            throw new DataEntryWriteFailedException(e);
        }
    }
}
