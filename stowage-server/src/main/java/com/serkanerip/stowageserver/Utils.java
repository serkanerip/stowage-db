package com.serkanerip.stowageserver;

import java.nio.file.Path;

class Utils {

    private Utils() {}

    public static long extractSegmentId(Path path) {
        return Long.parseLong(path.getFileName().toString().split(".data")[0]);
    }
}
