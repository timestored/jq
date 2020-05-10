package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;

import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;

public class HcountOp extends BaseMonad {
	public static HcountOp INSTANCE = new HcountOp();
	@Override public String name() { return "hcount"; }

    public Object run(Object o) {
    	try {
	    	if(o instanceof String) {
	    		return Files.size(HopenOp.toPath((String) o));
	    	}
    	} catch(IOException e) {
        	throw new DomainException(e);
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
}
