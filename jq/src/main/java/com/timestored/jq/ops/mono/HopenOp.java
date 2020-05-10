package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;

public class HopenOp extends BaseMonad {
	public static HopenOp INSTANCE = new HopenOp();
	@Override public String name() { return "hopen"; }

    public Object run(Object o) {
    	try {
	    	if(o instanceof String) {
	    		return Files.size(toPath((String) o));
	    	}
    	} catch(IOException e) {
        	throw new DomainException(e);
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
    
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
