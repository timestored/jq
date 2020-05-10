package com.timestored.jdb.server;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.StringMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import lombok.Getter;

/**
 * Receives a sequence of integers from a {@link FactorialClient} to calculate
 * the factorial of the specified integer.
 */
public final class TsdbServer {

    static final boolean SSL = System.getProperty("ssl") != null;
	private Channel f;
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	@Getter private int port;

    public TsdbServer(QueryHandler queryHandler, int port) {
    	Preconditions.checkNotNull(queryHandler);
    	Preconditions.checkArgument(port > 0);
    	this.port = port;
    	
        // Configure SSL.
        try {
	        final SslContext sslCtx;
	        if (SSL) {
	            SelfSignedCertificate ssc = new SelfSignedCertificate();
					sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
	        } else {
	            sslCtx = null;
	        }
	
	        bossGroup = new NioEventLoopGroup(1);
	        workerGroup = new NioEventLoopGroup();
	        try {
	            ServerBootstrap b = new ServerBootstrap();
	            b.group(bossGroup, workerGroup)
	             .channel(NioServerSocketChannel.class)
	             .handler(new LoggingHandler(LogLevel.INFO))
	             .childHandler(new TsdbServerInitializer(queryHandler, sslCtx));
	
	            f = b.bind(port).sync().channel();
	        } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		} catch (SSLException | CertificateException e) {
			throw new IllegalArgumentException("could not attach to port", e);
		} 
	}
    

    public void shutdown() throws InterruptedException {
    	f.close();
    	f.closeFuture().sync();
    	bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
    
    public static void main(String[] args) throws Exception {
    	new TsdbServer(null, 8322);
    }

}