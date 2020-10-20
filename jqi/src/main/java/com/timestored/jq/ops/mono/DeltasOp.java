package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.Database;

import static com.timestored.jdb.database.SpecialValues.*;
import com.timestored.jq.ops.CastOp;

public class DeltasOp extends MonadReduceToObject {
	public static DeltasOp INSTANCE = new DeltasOp();
	@Override public String name() { return "deltas"; }

	@Override public Object run(Object o) {
		if(Database.QCOMPATIBLE && 1 == CountOp.INSTANCE.count(o)) {
			return o;
		}
		return super.run(o);
	}
	
	@Override public IntegerCol ex(BooleanCol a) 	{ return ex(CastOp.CAST.i(a)); }
	@Override public IntegerCol ex(CharacterCol a){ return ex(CastOp.CAST.i(a)); }
	@Override public Col ex(ShortCol a)      {
		if(Database.QCOMPATIBLE) {
			return ex(CastOp.CAST.i(a));
		}
		return a.eachPrior((b, c) -> c == nh || b == nh ? nh : (short) (c-b)); 
	}
	@Override public IntegerCol ex(IntegerCol a)  {	return a.eachPrior((b, c) -> c == ni || b == ni ? ni : c-b); }
	@Override public LongCol ex(LongCol a) 		  {	return a.eachPrior((b, c) -> c == nj || b == nj ? nj : c-b); }
	@Override public FloatCol ex(FloatCol a) 	  {	return a.eachPrior((b, c) -> c == ne || b == ne ? ne : c-b); }
	@Override public DoubleCol ex(DoubleCol a) 	  {	return a.eachPrior((b, c) -> c == nf || b == nf ? nf : c-b); }
	
}
