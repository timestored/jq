package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.Op;

/** Notice sum/prd are VERY similar. Keep them in sync. **/
public class SumOp extends MonadReduceToObject {
	public static SumOp INSTANCE = new SumOp();
	@Override public String name() { return "sum"; }

	@Override public Object ex(BooleanCol a) 	{ return ex(CastOp.CAST.i(a)); }
	@Override public Object ex(CharacterCol a){ if(Database.QCOMPATIBLE) { return ex(CastOp.CAST.i(a)); } throw new TypeException(); }
	@Override public Object ex(ShortCol a) 			{ 
		return Database.QCOMPATIBLE ? 
			ex(CastOp.CAST.i(a)) 
			:  a.over((short) 0, (b, c) -> c == SpecialValues.nh ? b : (short) (b+c)); 
	}
	private static final Object to(Col toTypeCol, Object val) {
		return CastOp.CAST.run(toTypeCol.getType(), val);
	}
	@Override public Object ex(IntegerCol a)	{ return to(a, a.over(0, (b, c) -> c == SpecialValues.ni ? b : b+c)); }
	@Override public Object ex(LongCol a) 		{ return to(a, a.over(0, (b, c) -> c == SpecialValues.nj ? b : b+c)); }
	@Override public Float ex(FloatCol a) 		{ return a.over(0, (b, c) -> Float.isNaN(c) ? b : b+c); }
	@Override public Double ex(DoubleCol a) 	{ return a.over(0, (b, c) -> Double.isNaN(c) ? b : b+c); }
	
	@Override public Object ex(Op op) 		{ if(Database.QCOMPATIBLE) { return op; } throw new TypeException(); }
	@Override public Object ex(String a) 	{ if(Database.QCOMPATIBLE) { return a; } throw new TypeException(); }
	
	@Override public Object ex(ObjectCol o) {
		if(o.size() == 0) {
			return ColProvider.emptyCol((short)0);
		}
		if(FlipOp.getSquareCount(o) > 0) {
			return CastOp.flattenGenericIfSameType(mapEach((ObjectCol) FlipOp.INSTANCE.run(o)));
		}
		throw new LengthException("running functions can only be applied to typed or square array");
	}
	
	@Override public Object ex(Mapp o) {
		return new MyMapp(o.getKey(), (Col) run(o.getValue()));
	}
}
