package com.timestored.jq.ops.mono;

import static com.timestored.jdb.database.SpecialValues.nb;
import static com.timestored.jdb.database.SpecialValues.nc;
import static com.timestored.jdb.database.SpecialValues.nh;
import static com.timestored.jdb.database.SpecialValues.ni;
import static com.timestored.jdb.database.SpecialValues.nj;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;

public class FillsOp extends MonadReduceToSameObject {
	public static FillsOp INSTANCE = new FillsOp();
	@Override public String name() { return "fills"; }
	
	@Override public Object run(Object o) {
		long c = CountOp.INSTANCE.count(o);
		return c == 0 || c == 1 ? o : super.run(o);
	}
	
	@Override public Object ex(StringCol a) { return a.scan((b,c) -> c.length()==0 ? b : c); }
	@Override public BooleanCol ex(BooleanCol a) 	{ return a; }
	@Override public CharacterCol ex(CharacterCol a){ return a.scan((b,c) -> c == nc ? b : c); }
	@Override public ShortCol ex(ShortCol a)  		{ return a.scan((b,c) -> c == nh ? b : c); }
	@Override public IntegerCol ex(IntegerCol a)  { return a.scan((b,c) -> c == ni ? b : c); }
	@Override public LongCol ex(LongCol a) 		  { return a.scan((b,c) -> c == nj ? b : c); }
	@Override public FloatCol ex(FloatCol a) 	  { return a.scan((b,c) -> Float.isNaN(c) ? b : c); }
	@Override public DoubleCol ex(DoubleCol a) 	  { return a.scan((b,c) -> Double.isNaN(c) ? b : c); }

	@Override public double ex(double a){ return a; }
	@Override public float ex(float a) 	{ return a; }
	@Override public byte ex(byte a)	{ return a; }
	@Override public ByteCol ex(ByteCol a)	{ return a; }
	@Override public char ex(char a)	{ return a; }
	@Override public boolean ex(boolean a)	{ return a; }
	@Override public short ex(short a)	{ return a; }
	@Override public int ex(int a)		{ return a; }
	@Override public long ex(long a)	{ return a; }
	
}
