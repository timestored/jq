package com.timestored.jq.ops.mono;

import com.timestored.jdb.database.Database;
import com.timestored.jq.TypeException;

public class LtimeOp extends BaseMonad {
	public static LtimeOp INSTANCE = new LtimeOp();
	@Override public String name() {return "ltime"; }

	@Override public Object run(Object a) {
		if(Database.QCOMPATIBLE) {
			return reciprocalOp.INSTANCE.run(a);
		}
		throw new TypeException();
	}
}
