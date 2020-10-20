package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jq.ops.CastOp;

public class MaxOp extends MonadReduceToObject {
	public static MaxOp INSTANCE = new MaxOp();
	@Override public String name() { return "max"; }

	@Override public Boolean ex(BooleanCol a) { return a.max(); }
	@Override public Character ex(CharacterCol a) { return a.max(); }
	@Override public Short ex(ShortCol a) { return a.max(); }
	@Override public Object ex(IntegerCol a) { return CastOp.CAST.ex(a.getType(), a.max()); }
	@Override public Object ex(LongCol a) { return CastOp.CAST.ex(a.getType(), a.max()); }
	@Override public Object ex(FloatCol a) { return CastOp.CAST.ex(a.getType(), a.max()); }
	@Override public Object ex(DoubleCol a) { return CastOp.CAST.ex(a.getType(), a.max()); }
	@Override public Object ex(String a) { return a; }

}
