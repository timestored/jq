package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.SpecialValues;

public class MinsOp extends MonadReduceToObject {
	public static MinsOp INSTANCE = new MinsOp();
	@Override public String name() { return "mins"; }

	@Override public BooleanCol ex(BooleanCol a) 	{ return a.scan(true, (b, c) -> b && c); }
	@Override public CharacterCol ex(CharacterCol a){ return a.scan((b, c) -> (char) Math.min(b,c)); }
	@Override public ShortCol ex(ShortCol a) 		{ return a.scan(SpecialValues.wh, (b, c) -> c == SpecialValues.nh ? b : (short) Math.min(b,c)); }
	@Override public IntegerCol ex(IntegerCol a)	{ return a.scan(SpecialValues.wi, (b, c) -> c == SpecialValues.ni ? b : Math.min(b,c)); }
	@Override public LongCol ex(LongCol a) 			{ return a.scan(SpecialValues.wj, (b, c) -> c == SpecialValues.nj ? b : Math.min(b,c)); }
	@Override public FloatCol ex(FloatCol a) 		{ return a.scan(SpecialValues.we, (b, c) -> Float.isNaN(c) ? b : Math.min(b,c)); }
	@Override public DoubleCol ex(DoubleCol a) 		{ return a.scan(SpecialValues.wf, (b, c) -> Double.isNaN(c) ? b : Math.min(b,c)); }
	
}
