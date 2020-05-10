package com.timestored.jdb.server;

import java.nio.ByteOrder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MsgWrapperHandler extends SimpleChannelInboundHandler<MsgWrapper> {

	private final QueryHandler queryHandler;

	@Override protected void channelRead0(ChannelHandlerContext ctx, MsgWrapper msg) throws Exception {
			Object reply = null;
			try {
				reply = queryHandler.query(msg.getData());
			} catch(Exception e) {
				reply = e;
			}

			if(msg.getMsgType().equals(MsgType.SYNC)) {
				MsgWrapper replyMsg = new MsgWrapper(ByteOrder.BIG_ENDIAN, MsgType.RESPONSE, false, reply);
				ctx.writeAndFlush(replyMsg);
			}
	}

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.err.printf("TransferableServerHandler problem");
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
