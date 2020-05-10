package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;

public class Read0Op extends BaseMonad {
	public static Read0Op INSTANCE = new Read0Op();
	@Override public String name() { return "read0"; }

    public Object run(Object o) {
    	try {
	    	if(o instanceof String) {
	    		Path p = HopenOp.toRegularFilePath((String) o);
	    		List<String> a = Files.readAllLines(p);
	    		return ColProvider.toCharacterCol(a);
	    	}
    	} catch(IOException e) {
        	throw new DomainException(e);
    	}
    	throw new TypeException("Argument must be file path of format: `:filename or `filename");
    }
}
