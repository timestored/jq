package com.timestored.jq.ops.mono;

import com.timestored.jdb.database.Database;
import com.timestored.jq.TypeException;

public class HcloseOp extends BaseMonad {
	public static HcloseOp INSTANCE = new HcloseOp();
	@Override public String name() { return "hclose"; }

    public Object run(Object o) {
    	if(o instanceof Integer) {
    		context.closeHandle((int) o);
    		return o;
    	}
    	if(Database.QCOMPATIBLE) {
    		return IDescOp.INSTANCE.run(o);
    	}
    	throw new TypeException("hclose only works on integers as found in .z.handles");
    }
    
}
