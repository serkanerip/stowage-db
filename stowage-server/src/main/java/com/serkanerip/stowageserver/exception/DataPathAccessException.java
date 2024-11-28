package com.serkanerip.stowageserver.exception;

import java.nio.file.Path;

public class DataPathAccessException extends RuntimeException {
    public DataPathAccessException(Path path) {
        super(("The data root path '%s' is either not readable or not writable. "
            + "Ensure that the path has the correct permissions.").formatted(path));
    }

    public DataPathAccessException(String message, Exception cause) {
        super(message, cause);
    }
}