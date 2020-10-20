package com.timestored.jq.ops.mono;

import static com.timestored.jdb.database.SpecialValues.nf;

import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.database.Database;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class RatiosOp extends MonadReduceToObject {
	public static RatiosOp INSTANCE = new RatiosOp();
	@Override public String name() { return "ratios"; }

	@Override public Object run(Object o) {
		if(Database.QCOMPATIBLE && 1 == CountOp.INSTANCE.count(o)) {
			return o;
		}
		try {
			return ex((DoubleCol) CastOp.CAST.run((short) 9, o));
		} catch(ClassCastException cce) { }
		throw new TypeException();
	}
	@Override public DoubleCol ex(DoubleCol a) 	  {	return a.eachPrior((b, c) -> c == nf || b == nf ? nf : c/b); }
	
}
