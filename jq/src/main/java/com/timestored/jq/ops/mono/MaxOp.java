package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;

public class MaxOp extends MonadReduceToObject {
	public static MaxOp INSTANCE = new MaxOp();
	@Override public String name() { return "max"; }

	@Override public Boolean ex(BooleanCol a) { return a.max(); }
	@Override public Character ex(CharacterCol a) { return a.max(); }
	@Override public Short ex(ShortCol a) { return a.max(); }
	@Override public Integer ex(IntegerCol a) { return Database.QCOMPATIBLE && a.size() == 0 ? SpecialValues.ni : a.max(); }
	@Override public Long ex(LongCol a) { return Database.QCOMPATIBLE && a.size() == 0 ? SpecialValues.nj :  a.max(); }
	@Override public Float ex(FloatCol a) { return a.max(); }
	@Override public Double ex(DoubleCol a) { return a.max(); }
	
	@Override public Object ex(String a) { return a; }

}
