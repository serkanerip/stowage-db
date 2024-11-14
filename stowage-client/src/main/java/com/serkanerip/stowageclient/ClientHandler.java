package com.serkanerip.stowageclient;

import java.util.concurrent.ConcurrentMap;

import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.TransportMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    private final ConcurrentMap<Long, OngoingRequest> ongoingRequests;

    public ClientHandler(ConcurrentMap<Long, OngoingRequest> ongoingRequests) {
        this.ongoingRequests = ongoingRequests;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Client connected");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        TransportMessage transportMessage = (TransportMessage) msg;
        logger.debug("Received message of type: {} with size: {} uuid: {}",
            transportMessage.getType(), transportMessage.getSize(), transportMessage.getCorrelationId());
        var uuid = transportMessage.getCorrelationId();
        var request = ongoingRequests.remove(uuid);
        if (request != null) {
            var framePayload = switch (transportMessage.getType()) {
                case SIMPLE_RESPONSE -> SimpleResponse.decode(transportMessage.getPayload());
                case GET_RESPONSE -> GetResponse.decode(transportMessage.getPayload());
                default -> throw new IllegalStateException("Unexpected value: " + transportMessage.getType());
            };
            request.getFuture().complete(framePayload);
        } else {
            logger.error("Received response for unknown request with uuid: {}", uuid);
        }
        transportMessage.getPayload().release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unexpected exception from downstream", cause);
        ctx.close();
    }
}
