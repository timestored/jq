package com.timestored.jdb.server;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

import com.google.common.collect.Lists;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.StringMap;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.JUtils;
import com.timestored.jdb.database.Transferable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;

/**
 * Creates a newly configured {@link ChannelPipeline} for a server-side channel.
 */
@RequiredArgsConstructor
public class TsdbServerInitializer extends ChannelInitializer<SocketChannel> {

    private final QueryHandler queryHandler;
    private final SslContext sslCtx;

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

//        if (sslCtx != null) {
//            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
//        }
//
//        // Enable stream compression (you can remove these two if unnecessary)
//        pipeline.addLast(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
//        pipeline.addLast(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));

        // Add the number codec first,

        pipeline.addLast(new AuthHandler());
        
        pipeline.addLast(new ByteToMessageDecoder() {
			@Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		        in.markReaderIndex();
		        
//		        // DEBUG INFO
//		        byte[] b = new byte[in.readableBytes()];
//		        in.readBytes(b);
//		        System.out.println("rcvd = " + JUtils.toString(b));
//		        in.resetReaderIndex();
		        
		        MsgWrapper ipcWrapper = MsgWrapper.readFrom(in);
		        
		        if(ipcWrapper == null) {
		        	in.resetReaderIndex();
		        	return;
		        }
	        	out.add(ipcWrapper);
			}
		});
        
        pipeline.addLast(new MessageToByteEncoder<MsgWrapper>() {
            @Override protected void encode(ChannelHandlerContext ctx, MsgWrapper msg, ByteBuf out) {
                msg.writeTo(out);
            }
            
            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            	System.err.println(cause.toString());
            }
        });

        pipeline.addLast(new MsgWrapperHandler(queryHandler));
        
    }
}