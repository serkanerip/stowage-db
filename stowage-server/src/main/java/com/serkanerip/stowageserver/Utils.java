package com.serkanerip.stowageserver;

import java.nio.file.Path;

class Utils {

    private Utils() {}

    public static String extractSegmentId(Path path) {
        return path.getFileName().toString().split(".data")[0];
    }
}
