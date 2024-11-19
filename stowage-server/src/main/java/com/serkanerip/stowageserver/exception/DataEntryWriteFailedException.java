package com.serkanerip.stowageserver.exception;

public class DataEntryWriteFailedException extends RuntimeException {

    public DataEntryWriteFailedException(Throwable cause) {
        super(cause);
    }

    public DataEntryWriteFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
