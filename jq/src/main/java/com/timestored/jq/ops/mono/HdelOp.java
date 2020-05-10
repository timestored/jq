package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;

public class HdelOp extends BaseMonad {
	public static HdelOp INSTANCE = new HdelOp();
	@Override public String name() { return "hdel"; }

    public Object run(Object o) {
    	try {
	    	if(o instanceof String) {
	    		Path p = HopenOp.toRegularFilePath((String) o);
	    		Files.delete(p);
	    		return o;
	    	}
    	} catch(IOException e) {
        	throw new DomainException(e);
    	}
    	if(Database.QCOMPATIBLE) {
        	return NotOp.INSTANCE.run(o);	
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
}
