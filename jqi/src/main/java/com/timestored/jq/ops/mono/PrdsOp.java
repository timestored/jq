package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jq.ops.CastOp;

/** Notice sums/prds are VERY similar. Keep them in sync. **/
public class PrdsOp extends SumsOp {
	public static PrdsOp INSTANCE = new PrdsOp();
	@Override public String name() { return "prds"; }

	@Override public Col ex(ShortCol a) 			{ 
		return Database.QCOMPATIBLE ? 
			ex(CastOp.CAST.i(a)) 
			:  a.scan((short) 1, (b, c) -> c == SpecialValues.nh ? b : (short) (b*c)); 
	}
	@Override public IntegerCol ex(IntegerCol a)	{ return a.scan(1, (b, c) -> c == SpecialValues.ni ? b : b*c); }
	@Override public LongCol ex(LongCol a) 			{ return a.scan(1, (b, c) -> c == SpecialValues.nj ? b : b*c); }
	@Override public FloatCol ex(FloatCol a) 		{ return a.scan(1, (b, c) -> Float.isNaN(c) ? b : b*c); }
	@Override public DoubleCol ex(DoubleCol a) 		{ return a.scan(1, (b, c) -> Double.isNaN(c) ? b : b*c); }
	
}
