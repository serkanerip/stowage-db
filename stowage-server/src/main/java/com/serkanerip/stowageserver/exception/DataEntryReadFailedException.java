package com.serkanerip.stowageserver.exception;

public class DataEntryReadFailedException extends RuntimeException {

    public DataEntryReadFailedException(Throwable cause) {
        super(cause);
    }

    public DataEntryReadFailedException(String message) {
        super(message);
    }
}
