package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;

public class MaxsOp extends MonadReduceToObject {
	public static MaxsOp INSTANCE = new MaxsOp();
	@Override public String name() { return "maxs"; }

	@Override public BooleanCol ex(BooleanCol a) 	{ return a.scan((b, c) -> b || c); }
	@Override public CharacterCol ex(CharacterCol a){ return a.scan((b, c) -> (char) Math.max(b,c)); }
	@Override public ShortCol ex(ShortCol a) 		{ return a.scan(SpecialValues.nwh, (b, c) -> c == SpecialValues.nh ? b : (short) Math.max(b,c)); }
	@Override public IntegerCol ex(IntegerCol a)	{ return a.scan(SpecialValues.nwi, (b, c) -> c == SpecialValues.ni ? b : Math.max(b,c)); }
	@Override public LongCol ex(LongCol a) 			{ return a.scan(SpecialValues.nwj, (b, c) -> c == SpecialValues.nj ? b : Math.max(b,c)); }
	@Override public FloatCol ex(FloatCol a) 		{ return a.scan(SpecialValues.nwe, (b, c) -> Float.isNaN(c) ? b : Math.max(b,c)); }
	@Override public DoubleCol ex(DoubleCol a) 		{ return a.scan(SpecialValues.nwf, (b, c) -> Double.isNaN(c) ? b : Math.max(b,c)); }
	
}
