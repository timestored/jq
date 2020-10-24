package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.function.BooleanPairPredicate;

import static com.timestored.jdb.database.SpecialValues.*;
import com.timestored.jq.ops.CastOp;

public class DifferOp extends MonadReduceToObject {
	public static DifferOp INSTANCE = new DifferOp();
	@Override public String name() { return "differ"; }
	
	@Override public Object run(Object o) {
		if(1 == CountOp.INSTANCE.count(o)) {
			return false;
		}
		return super.run(o);
	}
	
	@Override public BooleanCol ex(BooleanCol a)  { return a.eachPrior(true, new BooleanPairPredicate() {
		@Override public boolean test(boolean a, boolean b) {
			return a != b;
		}
	}); }
	@Override public BooleanCol ex(CharacterCol a){ return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(ShortCol a)    { return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(IntegerCol a)  {	return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(LongCol a) 	  {	return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(FloatCol a) 	  {	return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(DoubleCol a)   {	return a.eachPrior(true, (b, c) -> c!=b); }
	@Override public BooleanCol ex(StringCol a)   {	return a.eachPrior(true, (b, c) -> !c.equals(b)); }
	
}
