package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jq.TypeException;

public class MinOp extends MonadReduceToObject {
	public static MinOp INSTANCE = new MinOp();
	@Override public String name() { return "min"; }

	@Override public Boolean ex(BooleanCol a) { return a.min(); }
	@Override public Character ex(CharacterCol a) { return a.min(); }
	@Override public Short ex(ShortCol a) { return a.min(); }
	@Override public Integer ex(IntegerCol a) { return a.min(); }
	@Override public Long ex(LongCol a) { return a.min(); }
	@Override public Float ex(FloatCol a) { return a.min(); }
	@Override public Double ex(DoubleCol a) { return a.min(); }
	
	@Override public Object ex(String a) { return a; }
}
