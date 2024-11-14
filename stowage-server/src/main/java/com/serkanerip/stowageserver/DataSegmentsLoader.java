package com.serkanerip.stowageserver;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataSegmentsLoader {

    private static final Logger log = LoggerFactory.getLogger(DataSegmentsLoader.class);

    private final ServerOptions options;

    DataSegmentsLoader(ServerOptions options) {
        this.options = options;
    }

    List<DataSegment> load() {
        var dataRootPath = options.dataRootPath();
        try {
            if (!dataRootPath.toFile().exists() && !dataRootPath.toFile().mkdirs()) {
                throw new RuntimeException();
            }
        } catch (Exception e) {
            log.error("Failed to create data root path, shutting down!", e);
            System.exit(1);
        }
        LinkedList<DataSegment> segments = new LinkedList<>();
        try {
            Files.list(dataRootPath).filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".data"))
                .sorted(Comparator.naturalOrder())
                .forEach(path -> segments.addFirst(new DataSegment(path, true)));
            return segments;
        } catch (IOException e) {
            log.error("Failed to load data segments", e);
            System.exit(1);
            throw new RuntimeException(e);
        }
    }

}
