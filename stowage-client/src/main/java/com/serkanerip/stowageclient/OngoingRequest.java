package com.serkanerip.stowageclient;

import java.util.concurrent.CompletableFuture;

import com.serkanerip.stowagecommon.MessagePayload;

class OngoingRequest {
    private final long requestId;
    private final CompletableFuture<MessagePayload> future;

    public OngoingRequest(long requestId) {
        this.requestId = requestId;
        this.future = new CompletableFuture<>();
    }

    public long getRequestId() {
        return requestId;
    }

    public CompletableFuture<MessagePayload> getFuture() {
        return future;
    }
}
