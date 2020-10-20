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

/** Notice sums/prds are VERY similar. Keep them in sync. **/
public class SumsOp extends MonadReduceToObject {
	public static SumsOp INSTANCE = new SumsOp();
	@Override public String name() { return "sums"; }

	@Override public IntegerCol ex(BooleanCol a) 	{ return ex(CastOp.CAST.i(a)); }
	@Override public CharacterCol ex(CharacterCol a){ throw new TypeException(); }
	@Override public Col ex(ShortCol a) 			{ 
		return Database.QCOMPATIBLE ? 
			ex(CastOp.CAST.i(a)) 
			:  a.scan((short) 0, (b, c) -> c == SpecialValues.nh ? b : (short) (b+c)); 
	}
	@Override public IntegerCol ex(IntegerCol a)	{ return a.scan(0, (b, c) -> c == SpecialValues.ni ? b : b+c); }
	@Override public LongCol ex(LongCol a) 			{ return a.scan(0, (b, c) -> c == SpecialValues.nj ? b : b+c); }
	@Override public FloatCol ex(FloatCol a) 		{ return a.scan(0, (b, c) -> Float.isNaN(c) ? b : b+c); }
	@Override public DoubleCol ex(DoubleCol a) 		{ return a.scan(0, (b, c) -> Double.isNaN(c) ? b : b+c); }
	
	@Override public Object ex(Op op) 		{ if(Database.QCOMPATIBLE) { return op; } throw new TypeException(); }
	@Override public Object ex(String a) 	{ if(Database.QCOMPATIBLE) { return a; } throw new TypeException(); }
	
	@Override public Object ex(ObjectCol o) {
		if(o.size() == 0) {
			return ColProvider.emptyCol((short)0);
		}
		if(FlipOp.getSquareCount(o) > 0) {
			return FlipOp.INSTANCE.run(mapEach((ObjectCol) FlipOp.INSTANCE.run(o)));
		}
		throw new LengthException("running functions can only be applied to typed or square array");
	}
	
	@Override public Object ex(Mapp o) {
		return new MyMapp(o.getKey(), (Col) run(o.getValue()));
	}
}
