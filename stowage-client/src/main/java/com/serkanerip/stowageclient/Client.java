package com.serkanerip.stowageclient;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;

public class Client {

    private NettyClient nettyClient = new NettyClient();

    public Client(String host, int port) {
        start(host, port);
    }

    // To be used in tests to be able to mock nettyClient
    Client(String host, int port, NettyClient nettyClient) {
        this.nettyClient = nettyClient;
        start(host, port);
    }

    private void start(String host, int port) {
        this.validateHostAndPort(Objects.requireNonNull(host), port);
        nettyClient.start(host, port);
    }

    public void shutdown() {
        nettyClient.shutdown();
    }

    public byte[] get(byte[] key) {
        var request = new GetRequest(Objects.requireNonNull(key, "Key cannot be null"));
        var ongoingRequest = nettyClient.sendMessage(request);
        try {
            var responsePayload = ongoingRequest.getFuture().get();
            if (responsePayload instanceof GetResponse response) {
                return response.getValue();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public boolean delete(byte[] key) {
        var request = new DeleteRequest(Objects.requireNonNull(key, "Key cannot be null"));
        var ongoingRequest = nettyClient.sendMessage(request);
        try {
            var responsePayload = ongoingRequest.getFuture().get();
            if (responsePayload instanceof SimpleResponse response) {
                return response.isSuccess();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public boolean put(byte[] key, byte[] value) {
        var payload = new PutRequest(
            Objects.requireNonNull(key, "Key cannot be null"),
            Objects.requireNonNull(value, "Value cannot be null")
        );
        var request = nettyClient.sendMessage(payload);
        try {
            var responsePayload = request.getFuture().get();
            if (responsePayload instanceof SimpleResponse sr) {
                return sr.isSuccess();
            }
            throw new IllegalStateException("Unexpected response type: " + responsePayload.getType());
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void validateHostAndPort(String host, int port) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
    }

}
