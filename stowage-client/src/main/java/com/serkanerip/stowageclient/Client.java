package com.serkanerip.stowageclient;

import java.util.concurrent.ExecutionException;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.HeapData;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;

public class Client {

    private final NettyClient nettyClient;

    public Client(String host, int port) {
        this.nettyClient = new NettyClient();
        nettyClient.start(host, port);
    }

    public void shutdown() {
        nettyClient.shutdown();
    }

    public byte[] get(byte[] key) {
        var request = new GetRequest(new HeapData(key));
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
        var request = new DeleteRequest(new HeapData(key));
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
        var payload = new PutRequest(new HeapData(key), new HeapData(value));
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

}
