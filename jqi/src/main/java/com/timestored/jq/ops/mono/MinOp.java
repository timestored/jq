package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class MinOp extends MonadReduceToObject {
	public static MinOp INSTANCE = new MinOp();
	@Override public String name() { return "min"; }

	@Override public Boolean ex(BooleanCol a) { return a.min(); }
	@Override public Character ex(CharacterCol a) { return a.min(); }
	@Override public Short ex(ShortCol a) { return a.min(); }
	@Override public Object ex(IntegerCol a)	{ return CastOp.CAST.ex(a.getType(), a.min()); }
	@Override public Object ex(LongCol a)		{ return CastOp.CAST.ex(a.getType(), a.min()); }
	@Override public Object ex(FloatCol a)		{ return CastOp.CAST.ex(a.getType(), a.min()); }
	@Override public Object ex(DoubleCol a)		{ return CastOp.CAST.ex(a.getType(), a.min()); }
	
	@Override public Object ex(String a) { return a; }
}
