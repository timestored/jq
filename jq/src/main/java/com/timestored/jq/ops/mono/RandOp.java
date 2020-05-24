package com.timestored.jq.ops.mono;

import java.util.concurrent.ThreadLocalRandom;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.ops.CastOp;

import lombok.Setter;

public class RandOp extends MonadReduceToObject {
	public static final RandOp INSTANCE = new RandOp();
//	private static final Random r = new Random();
	@Setter private boolean debug = false;
	
	@Override public Object run(Object o) {
		if(debug) {
			return 1;
		}
		return super.run(o);
	}
	
	@Override public String name() { return "rand"; }
	
	private ThreadLocalRandom r() { return ThreadLocalRandom.current(); }

	@Override public Integer ex(int a) {return a == 0 ? r().nextInt() : r().nextInt(a); }
	@Override public Boolean ex(boolean a) {return a ? false : r().nextBoolean(); }
	@Override public Float ex(float a) {return (a == 0 ? r().nextFloat() : (float) r().nextDouble(a)); }
	@Override public Long ex(long a) {return a == 0 ? r().nextLong() : r().nextLong(a); }
	@Override public Character ex(char a) {return (char) ((char) a == 0 ? r().nextInt(1 << 16) : r().nextInt(Math.min(1 << 16, a))) ; }
	@Override public Short ex(short a) {return (short) (char) ex(CastOp.CAST.c(a)); }
	@Override public Double ex(double a) {return (a == 0 ? r().nextDouble() : (float) r().nextDouble(a)); }
	
	@Override public Long ex(LongCol a)			{ return a.get(r().nextInt(a.size())); }
	@Override public Character ex(CharacterCol a) { return a.get(r().nextInt(a.size())); }
	@Override public Float ex(FloatCol a)		{ return a.get(r().nextInt(a.size())); }
	@Override public Integer ex(IntegerCol a)	{ return a.get(r().nextInt(a.size())); }
	@Override public Short ex(ShortCol a)		{ return a.get(r().nextInt(a.size())); }
	@Override public Double ex(DoubleCol a)		{ return a.get(r().nextInt(a.size())); }
	@Override public Boolean ex(BooleanCol a)	{ return a.get(r().nextInt(a.size())); }
	
	@Override public Object ex(ObjectCol a) 	{ return a.get(r().nextInt(a.size())); }
	@Override public Object ex(StringCol a)		{ return a.get(r().nextInt(a.size())); }
	
	@Override public Object ex(String a) {
		try {
			int stringLength = Integer.parseInt(a);
			if(stringLength < 1 || stringLength > 999) {
				throw new DomainException("Rand only works for strings lengths 2-999");
			}
			return generateRandomLetterString(stringLength);
		} catch(NumberFormatException nfe) {}
		throw new DomainException("Rand only works for strings lengths 2-999");
	}
	
	public String generateRandomLetterString(int stringLength) {
	    int leftLimit = 97; // letter 'a'
	    int rightLimit = 122; // letter 'z'
	 
	    return r().ints(leftLimit, rightLimit + 1)
	      .limit(stringLength)
	      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
	      .toString();
	}

}
