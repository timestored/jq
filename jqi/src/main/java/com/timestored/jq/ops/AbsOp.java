package com.timestored.jq.ops; 

import com.timestored.jdb.database.Database;
import com.timestored.jq.ops.mono.NaiveAbsOp;
import com.timestored.jq.ops.mono.TypeOp;


public class AbsOp extends NaiveAbsOp {
	public static AbsOp INSTANCE = new AbsOp();
	@Override public String name() { return "abs"; }
	
	@Override public Object run(Object a) {
		// Convert boolean/chars to int
		int typ = Math.abs(TypeOp.TYPE.type(a));
		if(Database.QCOMPATIBLE && typ==1 || typ==10) {
			return super.run(CastOp.CAST.run((short) 6, a));
		}
		return super.run(a);
	}
}