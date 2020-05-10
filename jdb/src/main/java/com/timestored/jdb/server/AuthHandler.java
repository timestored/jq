package com.timestored.jdb.server;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.java.Log;

@Log
public class AuthHandler extends ByteToMessageDecoder {

	@Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

		  // Wait until the length prefix is available.
		int sz = in.readableBytes();
        if (sz < 3) {
            return;
        }

        in.markReaderIndex();
        byte[] data = new byte[sz];
        in.readBytes(data);
        
        int i=0;
        while(i<sz && data[i]>0) {
        	i++;
        }
        if(i == sz) {
        	// no end of string detected return
        	in.resetReaderIndex();
        	return;
        } 

    	char[] usernamePassword = new char[i - 1];
    	byte kdbCommVersion = data[i-1];
    	for(int j=0; j<i-1; j++) {
    		usernamePassword[j] = (char) data[j];
    	}
    	log.fine("username and password was: " + new String(usernamePassword) + " kdbCommVersion: " + kdbCommVersion);
    	ByteBuf outBB = ctx.alloc().buffer(1);
    	outBB.writeByte(3); // comm version
    	ctx.write(outBB);
    	ctx.flush();
    	// now finished authorizing so remove us
    	ctx.pipeline().remove(this);
	}
}