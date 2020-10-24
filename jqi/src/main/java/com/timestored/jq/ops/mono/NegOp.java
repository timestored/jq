package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.database.Database;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.MulOp;

public class NegOp extends BaseMonad {
	public static NegOp INSTANCE = new NegOp();
	@Override public String name() { return "neg"; }

    public Object run(Object o) {
    	if(Database.QCOMPATIBLE) {
    		if(o instanceof CharacterCol) {
    			return MulOp.INSTANCE.run(-1, CastOp.CAST.i((CharacterCol)o));
    		}
    	}
    	return MulOp.INSTANCE.run(-1, o);
    }
}
