package com.serkanerip.stowageserver;

import com.serkanerip.stowagecommon.DeleteRequest;
import com.serkanerip.stowagecommon.ErrorCode;
import com.serkanerip.stowagecommon.GetRequest;
import com.serkanerip.stowagecommon.PutRequest;
import com.serkanerip.stowagecommon.SimpleResponse;
import com.serkanerip.stowagecommon.TransportMessage;
import com.serkanerip.stowagecommon.TransportMessageType;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class ServerInboundHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ServerInboundHandler.class);

    private final ChannelGroup channelGroup;

    private final StoreQueue storeQueue;

    public ServerInboundHandler(ChannelGroup channelGroup, StoreQueue storeQueue) {
        this.channelGroup = channelGroup;
        this.storeQueue = storeQueue;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Channel active");
        channelGroup.add(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception caught, closing the client channel!", cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        var message = (TransportMessage) msg;
        var type = message.getType();
        var size = message.getSize();

        logger.debug("Received frame of type: {} with size: {} uuid: {}",
            type, size, message.getCorrelationId());
        var payload = message.getPayload();
        switch (message.getType()) {
            case PUT -> {
                var request = PutRequest.decode(payload);
                storeQueue.addToQueue(request, message.getCorrelationId(), ctx.channel());
            }
            case GET -> {
                var request = GetRequest.decode(payload);
                storeQueue.addToQueue(request, message.getCorrelationId(), ctx.channel());
            }
            case DELETE -> {
                var request = DeleteRequest.decode(payload);
                storeQueue.addToQueue(request, message.getCorrelationId(), ctx.channel());
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
}
