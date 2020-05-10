package com.timestored.jq.ops;

import java.util.Arrays;
import java.util.function.Function;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.JUtils;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.TypeException;

/** Parse is the basically reverse of StringOp **/
public class ParseOp extends BaseDiad {
	public static final ParseOp PARSE = new ParseOp();
	@Override public String name() { return "$"; }
	
	@Override public Object run(Object a, Object b) {
		if(a instanceof Character) {
			if(b instanceof Character) {
				return parse((char) a, ""+(char)b, true);
			} else if(b instanceof CharacterCol) {
				return parse((char) a, CastOp.CAST.s((CharacterCol) b), true);
			}
		}
		throw new NYIException();
	}

	public Col parse(char toTypeChar, String[] b, boolean replaceErrorsWithNull) {
		switch(Character.toUpperCase(toTypeChar)) {
		case 'B': return b(b, replaceErrorsWithNull);
		case 'C': return c(b, replaceErrorsWithNull);
		case 'P': return p(b, replaceErrorsWithNull);
		case 'M': return m(b, replaceErrorsWithNull);
		case 'D': return d(b, replaceErrorsWithNull);
		case 'N': return n(b, replaceErrorsWithNull);
		case 'U': return u(b, replaceErrorsWithNull);
		case 'V': return v(b, replaceErrorsWithNull);
		case 'T': return t(b, replaceErrorsWithNull);
		case 'H': return h(b, replaceErrorsWithNull);
		case 'I': return i(b, replaceErrorsWithNull);
		case 'J': return j(b, replaceErrorsWithNull);
		case 'E': return e(b, replaceErrorsWithNull);
		case 'F': return f(b, replaceErrorsWithNull);
		}
		throw new NYIException();
	}

	public Object parse(char toTypeChar, String b, boolean replaceErrorsWithNull) {
		switch(Character.toUpperCase(toTypeChar)) {
		case 'X':
			byte[] r = x(b, replaceErrorsWithNull);
			return r.length == 1 ? r[0] : r;
		case 'B': return b(b, replaceErrorsWithNull);
		case 'C': return c(b, replaceErrorsWithNull);
		case 'P': return p(b, replaceErrorsWithNull);
		case 'M': return m(b, replaceErrorsWithNull);
		case 'D': return d(b, replaceErrorsWithNull);
		case 'N': return n(b, replaceErrorsWithNull);
		case 'U': return u(b, replaceErrorsWithNull);
		case 'V': return v(b, replaceErrorsWithNull);
		case 'T': return t(b, replaceErrorsWithNull);
		case 'H': return h(b, replaceErrorsWithNull);
		case 'I': return i(b, replaceErrorsWithNull);
		case 'J': return j(b, replaceErrorsWithNull);
		case 'E': return e(b, replaceErrorsWithNull);
		case 'F': return f(b, replaceErrorsWithNull);
		}
		throw new NYIException();
	}


	/**
	 * Public functions should never hide exceptions
	 */
	public static byte[] x(String t)  { return x(t, false); }
	public static boolean b(String t) { return b(t, false); }
	public static Month m(String t)   { return m(t, false); }
	public static Dt d(String t)      { return d(t, false); }
	public static Timstamp p(String t){ return p(t, false); }
	public static Timespan n(String t){ return n(t, false); }
	public static Minute u(String t)  { return u(t, false); }
	public static Second v(String t)  { return v(t, false); }
	public static Time t(String t)    { return t(t, false); }
	public static LongCol p(String[] t)  	{ return p(t, false); }
	public static IntegerCol m(String[] t)      { return m(t, false); }
	public static IntegerCol d(String[] t)      { return d(t, false); }
	public static LongCol n(String[] t)  	{ return n(t, false); }
	public static IntegerCol u(String[] t)  { return u(t, false); }
	public static IntegerCol v(String[] t)  { return v(t, false); }
	public static IntegerCol t(String[] t)    { return t(t, false); }


	private static IntegerCol getIntCol(String[] s, char typLetter, final Function<String, Integer> toInt, boolean replaceErrorsWithNull) {
		MemoryIntegerCol vals = new MemoryIntegerCol(s.length);
		for (int i = 0; i < s.length; i++) {
			int val = SpecialValues.ni;
			try {
				val = toInt.apply(s[i]);
			} catch(RuntimeException e) {
				if(!replaceErrorsWithNull) {
					throw new TypeException("Couldn't parse " + Arrays.deepToString(s));
				}
			}
			vals.set(i, val);
		}
		vals.setSorted(false);
		vals.setType((short) (-1 * CType.getType(typLetter).getTypeNum()));
		return vals;
	}


	private static LongCol getLongCol(String[] s, char typLetter, final Function<String, Long> toLong, boolean replaceErrorsWithNull) {
		MemoryLongCol vals = new MemoryLongCol(s.length);
		for (int i = 0; i < s.length; i++) {
			long val = SpecialValues.nj;
			try {
				val = toLong.apply(s[i]);
			} catch(RuntimeException e) {
				if(!replaceErrorsWithNull) {
					throw new TypeException("Couldn't parse " + Arrays.deepToString(s));
				}
			}
			vals.set(i, val);
		}
		vals.setSorted(false);
		vals.setType((short) (-1 * CType.getType(typLetter).getTypeNum()));
		return vals;
	}
	
	private static byte[] x(String t, boolean replaceErrorsWithNull) {
		try {
			return JUtils.decode(t);
		} catch(IllegalArgumentException e) {
			if(replaceErrorsWithNull) {
				return new byte[0];
			}
			throw new TypeException("couldn't parse: " + t);
		}
	}
	
	private static boolean b(String t, boolean replaceErrorsWithNull) {
		if(t.equals("1")) {
			return true;
		} else if(t.equals("0")) {
			return false;
		}
		if(replaceErrorsWithNull) {
			return false;
		}
		throw new TypeException("couldn't parse: " + t);
	}
	private static char c(String t, boolean replaceErrorsWithNull) {
		String s = t.trim().toLowerCase();
		if(s.length() == 0 || s.equals("0n") || s.equals("0w") || s.equals("-0w")) {
			return SpecialValues.nc;
		} else if(t.length() == 1) {
			return t.charAt(0);  
		} else if(replaceErrorsWithNull) {
			return SpecialValues.nc;
		}
		throw new TypeException("couldn't parse: " + t);
	}
	
	private static Dt d(String text, boolean replaceErrorsWithNull) {
		try {
			return Dt.valueOf(text);
		} catch(IllegalArgumentException e) {
			if(replaceErrorsWithNull) {
				return SpecialValues.nd;
			}
			throw new TypeException(e.toString());
		}
	}
	
	private static Month m(String text, boolean replaceErrorsWithNull) {
		try {
			return Month.valueOf(text);
		} catch(IllegalArgumentException e) {
			if(replaceErrorsWithNull) {
				return SpecialValues.nm;
			}
			throw new TypeException(e.toString());
		}
	}
	
	private static IntegerCol m(String[] sp, boolean replaceErrorsWithNull) { 
		return getIntCol(sp, 'm', s -> Month.valueOf(s).getInt(), replaceErrorsWithNull);
	}
	
	private static IntegerCol d(String[] sp, boolean replaceErrorsWithNull) { 
		return getIntCol(sp, 'd', s -> Dt.valueOf(s).getInt(), replaceErrorsWithNull); 
	}
	private static Timespan n(String text, boolean replaceErrorsWithNull) { 
		return Timespan.valueOf(text); 
	}
	private static Timstamp p(String text, boolean replaceErrorsWithNull) { 
		return Timstamp.valueOf(text); 
	}
	private static Minute u(String text, boolean replaceErrorsWithNull) { 
		return Minute.valueOf(text); 
	}
	private static LongCol p(String[] sp, boolean replaceErrorsWithNull) { 
		return getLongCol(sp, 'p', s -> Timstamp.valueOf(s).getLong(), replaceErrorsWithNull); 
	}
	private static LongCol n(String[] sp, boolean replaceErrorsWithNull) { 
		return getLongCol(sp, 'n', s -> Timespan.valueOf(s).getLong(), replaceErrorsWithNull); 
	}
	private static IntegerCol u(String[] sp, boolean replaceErrorsWithNull) { 
		return getIntCol(sp, 'u', s -> Minute.valueOf(s).getInt(), replaceErrorsWithNull); 
	}
	private static Second v(String text, boolean replaceErrorsWithNull) { 
		return Second.valueOf(text); 
	}
	private static IntegerCol v(String[] sp, boolean replaceErrorsWithNull) { 
		return getIntCol(sp, 'v', s -> Second.valueOf(s).getInt(), replaceErrorsWithNull); 
	}
	private static Time t(String text, boolean replaceErrorsWithNull) { 
		return Time.valueOf(text); 
	}
	private static IntegerCol t(String[] sp, boolean replaceErrorsWithNull) { 
		return getIntCol(sp, 't', s -> Time.valueOf(s).getInt(), replaceErrorsWithNull); 
	}
	
	
	
	
	
	

	private static CharacterCol c(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.c(s.length, i -> c(s[i].trim(), replaceErrorsWithNull));
	}
	
	private static BooleanCol b(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.b(s.length, i -> b(s[i].trim(), replaceErrorsWithNull));
	}

	private static ShortCol h(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.h(s.length, i -> h(s[i].trim(), replaceErrorsWithNull));
	}
	
	private static IntegerCol i(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.i(s.length, i -> i(s[i].trim(), replaceErrorsWithNull));
	}

	private static LongCol j(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.j(s.length, i -> j(s[i].trim(), replaceErrorsWithNull));
	}

	private static FloatCol e(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.e(s.length, i -> e(s[i].trim(), replaceErrorsWithNull));
	}

	private static DoubleCol f(String[] s, boolean replaceErrorsWithNull) {
		return ColProvider.f(s.length, i -> f(s[i].trim(), replaceErrorsWithNull));
	}

	
	private static short h(String t, boolean replaceErrorsWithNull) {
		switch (t.trim().toLowerCase()) {
		case "0w": return SpecialValues.wh;
		case "-0w": return SpecialValues.nwh;
		case "0n": return SpecialValues.nh;
		}
		try {
			return Short.parseShort(t);
		} catch(NumberFormatException nfe) { 
			if(!replaceErrorsWithNull) {
				throw new TypeException("couldn't parse: " + t);
			}
		}
		return SpecialValues.nh;
	}

	private static int i(String t, boolean replaceErrorsWithNull) {
		switch (t.trim().toLowerCase()) {
		case "0w": return SpecialValues.wi;
		case "-0w": return SpecialValues.nwi;
		case "0n": return SpecialValues.ni;
		}
		try {
			return Integer.parseInt(t);
		} catch(NumberFormatException nfe) { 
			if(!replaceErrorsWithNull) {
				throw new TypeException("couldn't parse: " + t);
			}
		}
		return SpecialValues.ni;
	}

	private static long j(String t, boolean replaceErrorsWithNull) {
		switch (t.trim().toLowerCase()) {
		case "0w": return SpecialValues.wj;
		case "-0w": return SpecialValues.nwj;
		case "0n": return SpecialValues.nj;
		}
		try {
			return Long.parseLong(t);
		} catch(NumberFormatException nfe) { 
			if(!replaceErrorsWithNull) {
				throw new TypeException("couldn't parse: " + t);
			}
		}
		return SpecialValues.nj;
	}
	
	private static float e(String t, boolean replaceErrorsWithNull) {
		switch (t.trim().toLowerCase()) {
		case "0w": return SpecialValues.we;
		case "-0w": return SpecialValues.nwe;
		case "0n": return SpecialValues.ne;
		}
		try {
			return Float.parseFloat(t);
		} catch(NumberFormatException nfe) { 
			if(!replaceErrorsWithNull) {
				throw new TypeException("couldn't parse: " + t);
			}
		}
		return SpecialValues.ne;
	}

	private static double f(String t, boolean replaceErrorsWithNull) {
		switch (t.trim().toLowerCase()) {
		case "0w": return SpecialValues.wf;
		case "-0w": return SpecialValues.nwf;
		case "0n": return SpecialValues.nf;
		}
		try {
			return Double.parseDouble(t);
		} catch(NumberFormatException nfe) { 
			if(!replaceErrorsWithNull) {
				throw new TypeException("couldn't parse: " + t);
			}
		}
		return SpecialValues.ne;
	}

}