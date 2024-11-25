package com.serkanerip.stowageserver;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.ErrorCode;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.GetResponse;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class ServerInboundHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ServerInboundHandler.class);

    private final StowageDB store;

    public ServerInboundHandler(StowageDB store) {
        this.store = store;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Channel active");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception caught, closing the client channel!", cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        var message = (TransportMessage) msg;
        handleMessage(ctx, message);
    }

    private void handleMessage(ChannelHandlerContext ctx, TransportMessage message) {
        var type = message.getType();
        var size = message.getSize();

        logger.debug("Received message of type: {} with size: {} uuid: {}",
            type, size, message.getCorrelationId());
        var payload = message.getPayload();
        switch (message.getType()) {
            case PUT -> {
                var request = PutRequest.decode(payload);
                store.put(request.getKey(), request.getValue());
                sendResponse(ctx.channel(), new TransportMessage(
                    TransportMessageType.SIMPLE_RESPONSE,
                    message.getCorrelationId(),
                    // TODO use a static instance of this response instead of creating new on each response
                    new SimpleResponse(true, ErrorCode.NO_ERR).encode()
                ));
            }
            case GET -> {
                var request = GetRequest.decode(payload);
                var value = store.get(request.getKey());
                sendResponse(ctx.channel(), new TransportMessage(
                    TransportMessageType.GET_RESPONSE,
                    message.getCorrelationId(),
                    new GetResponse(value).encode()
                ));
            }
            case DELETE -> {
                var request = DeleteRequest.decode(payload);
                store.delete(request.getKey());
                sendResponse(ctx.channel(), new TransportMessage(
                    TransportMessageType.SIMPLE_RESPONSE,
                    message.getCorrelationId(),
                    // TODO use a static instance of this response instead of creating new on each response
                    new SimpleResponse(true, ErrorCode.NO_ERR).encode()
                ));
            }
            default -> {
                logger.error("Received message of unknown type: {}", type);
                var simpleResponse = new SimpleResponse(false, ErrorCode.INVALID_FRAME);
                ctx.writeAndFlush(new TransportMessage(
                    TransportMessageType.SIMPLE_RESPONSE, message.getCorrelationId(), simpleResponse.encode()
                ));
                ctx.close();
            }
        }
        payload.release();
    }

    private void sendResponse(Channel channel, TransportMessage message) {
        if (channel.isActive()) {
            channel.writeAndFlush(message);
        } else {
            logger.warn("Channel is not active, can't send the response!");
        }
    }
}
