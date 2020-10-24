package com.timestored.jq.ops.mono;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class HopenOp extends BaseMonad {
	public static HopenOp INSTANCE = new HopenOp();
	@Override public String name() { return "hopen"; }

    public Object run(Object o) {
    	try {
    		String s = o instanceof String ? ((String)o) : o instanceof CharacterCol ? CastOp.CAST.s((CharacterCol)o) : null; 
	    	if(s != null) {
	    		if(CharMatcher.is(':').countIn(s) >= 2) {
	    			
	    			Socket sk = toSocket(s);
	    			BufferedOutputStream bo = new BufferedOutputStream(sk.getOutputStream());
		    		return context.addHandle(bo, sk);
	    		} else {
		    		File f = toPath(s).toFile();
		    		FileOutputStream fos = new FileOutputStream(f, true);
		    		BufferedOutputStream bo = new BufferedOutputStream(fos);
		    		return context.addHandle(bo, f, fos.getFD());
	    		}
	    	}
    	} catch(IOException e) {
        	throw new DomainException(e);
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
    
	private Socket toSocket(String s) throws UnknownHostException, IOException {
		String t = s.startsWith(":") ? s.substring(1) : s;
		ImmutableList<String> splt = ImmutableList.copyOf(Splitter.on(':').split(t));
		int p = s.indexOf(':');
		Preconditions.checkArgument(splt.size() >= 2);
		String host = splt.get(0);
		int port = Integer.parseInt(splt.get(1));
		String username = splt.size() > 2 ? splt.get(2) : "";
		String password = splt.size() > 3 ? splt.get(3) : "";
		Socket socket = new Socket(host, port);
		// @TODO do kdb signin 
		return socket;
	}

	/**
	 * Gets a java Path from a k format path and checks that it exists, is a file and is readable.
	 */
	public static Path toRegularFilePath(String kFilePath) {
		Path p = toPath(kFilePath);
		if(!Files.exists(p)) { throw new DomainException("File doesn't exist:" + p); }
		if(!Files.isReadable(p)) { throw new DomainException("File isn't readable:" + p); }
		if(!Files.isRegularFile(p) ) { throw new DomainException("File isn't regular file:" + p); }
		return p;
	}
	
	public static Path toPath(String kFilePath) {
		String s = kFilePath.startsWith(":") ? kFilePath.substring(1) : kFilePath;
		return Paths.get(s);
	}
}
