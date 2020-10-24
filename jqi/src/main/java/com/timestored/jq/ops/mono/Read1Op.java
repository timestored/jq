package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;

import com.timestored.jdb.col.MemoryByteCol;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jq.TypeException;

public class Read1Op extends BaseMonad {
	public static Read1Op INSTANCE = new Read1Op();
	@Override public String name() { return "read1"; }

    public Object run(Object o) {
    	try {
	    	if(o instanceof String) {
	    		byte[] vals = Files.readAllBytes(HopenOp.toRegularFilePath((String) o));
	    		return new MemoryByteCol(vals);
	    	}
    	} catch(IOException e) {
        	throw new OsException(e);
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
}
