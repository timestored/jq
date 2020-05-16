package com.timestored.jq.ops.mono;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jq.ops.EachOp;
import com.timestored.jq.ops.EqualOp;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.Op;

/** String is the basically reverse of {@link ParseOp} except nulls are "" **/
public class StringOp extends MonadReduceToObject {
	public static StringOp INSTANCE = new StringOp();
	@Override public String name() { return "string"; }
    
	public String asString(Object o) {
		return (String) super.run(o);
	}
	

    @Override public Object ex(ObjectCol a) { return mapEach(a); }
	
	@Override public Object run(Object o) {
		// bale out early for (();();()) as can't recurse into it.
		if(o instanceof ObjectCol) {
			ObjectCol oc = (ObjectCol) o;
			if((oc.size() == 0)) {
				return "";
			}
			if(isAllEmptyObjectCol(o)) {
				String s = "";
				for(int i=1; i<oc.size(); i++) {
					s += "\r\n";
				}
				return s;
			}
		}
		Object r = super.run(o);
		if(r instanceof String) {
			return ColProvider.toCharacterCol(((String) r));	
		}
		return r;
	}

	static boolean isAllEmptyObjectCol(Object o) {
		return (o instanceof ObjectCol) && 
				(boolean) AllOp.INSTANCE.run(EqualOp.INSTANCE.run(0,EachOp.INSTANCE.run(CountOp.INSTANCE, o)));
	}

    private static double getFloatAsDouble(float value) {
        return Double.valueOf(Float.valueOf(value).toString()).doubleValue();
    }
    
    /**
     * Naive casting of floats to doubles doesn't round properly, try 6.2 in both to see difference.
     * DecimalFormat only supports doubles. 
     */
    private String formatFloatingPt(float d) {
		return formatFloatingPt(getFloatAsDouble(d));
	}
	
    private String formatFloatingPt(double d) {
		return getDF(d).format(d).replace("E-", "e-").replace("E", "e+").replace("e+000", "");
	}

	private DecimalFormat getDF(double d) {
		int decimalPlaces = QsOp.INSTANCE.getPrecision();
		// WARNING this is negative for small numbers
		int length = (int) (Math.log10(d) + 1);
		DecimalFormat df = null;
		// Whole numbers show , else slightly small show all decimal places, otherwise scientific
		boolean isTiny = Math.abs(d) > 0 && Math.abs(d)<0.0001;
		if(length > decimalPlaces || isTiny) {
			df = new DecimalFormat("0." + hash(decimalPlaces - 1) + "E000");
		} else {
			int l = (length >= 0 ? length : 0);
			String deccy = l<decimalPlaces ? ("." + hash(decimalPlaces-l)) : "";
			df = new DecimalFormat(hash(l > 1 ? l-1 : 0) + "0" +  deccy);
			df.setRoundingMode(RoundingMode.HALF_UP);
		}
		return df;
	}

    private static String hash(int count) {
    	Preconditions.checkArgument(count >= 0);
    	String s = "";
    	for(int i = 0; i<count; i++) {
    		s += "#";
    	}
    	return s;
    }
	
    public String ex(String a)  { return a; }
    public String ex(boolean a) { return a ? "1" : "0"; }
    public String ex(char a)    { return ""+a; }
    public String ex(short a)   { 
    	return a == SpecialValues.nh ? "" : ""+a; 
    }
    
    public String ex(int a)     { 
    	return a == SpecialValues.ni ? ""
    		: a == SpecialValues.wi ? "0W"
    		: a == SpecialValues.nwi ? "-0W"
    		: ""+a;
    }
    
    public String ex(long a) {
    	return a == SpecialValues.nj ? ""
    		: a == SpecialValues.wj ? "0W"
    		: a == SpecialValues.nwj ? "-0W"
    		: ""+a; 
    }
    
    public String ex(float a) {
    	return Float.isNaN(a) ? ""
    		: Float.isInfinite(a) ? (a > 0 ? "0w" : "-0w")
    		: formatFloatingPt(a); 
    }
    public String ex(double a) {
    	return Double.isNaN(a) ? ""
    		: Double.isInfinite(a) ? (a > 0 ? "0w" : "-0w")
    		: formatFloatingPt(a); 
    }
    
    
    
    @Override public ObjectCol ex(BooleanCol a) {
    	ObjectCol r = new MemoryObjectCol(a.size());
    	for(int i=0; i<a.size(); i++) {
    		r.set(i, ColProvider.toCharacterCol(ex(a.get(i))));
    	}
    	return r; 
    }
    
    private ObjectCol mapEachOne(Col c, Function<Object,String> map) {
    	ObjectCol r = new MemoryObjectCol(c.size());
    	for(int i=0; i<c.size(); i++) {
    		String s = map.apply(IndexOp.INSTANCE.run(c, i));
    		r.set(i, ColProvider.toCharacterCol(s));
    	}
    	return r; 
    }


    @Override public ObjectCol ex(ByteCol a) { return mapEachOne(a, o -> ex((byte) (Byte)o)); }
    @Override public String ex(byte a)   {  return String.format("%02X", a); }
	 
    @Override public ObjectCol ex(ShortCol a)  { return mapEachOne(a, o -> ex((short) (Short)o)); }
    @Override public ObjectCol ex(FloatCol a)  { return mapEachOne(a, o -> ex((float) (Float)o)); }
    @Override public ObjectCol ex(DoubleCol a) { return mapEachOne(a, o -> ex((double) (Double)o)); }
    @Override public ObjectCol ex(CharacterCol a) { return mapEachOne(a, o -> ex((char) (Character)o)); }
    @Override public ObjectCol ex(StringCol a) { 	return mapEachOne(a, o -> ex((String)o)); }

    @Override public ObjectCol ex(IntegerCol a){
    	switch(CType.getType(a.getType())) {
    	case MINUTE: return mapEachOne(a, o -> ex((Minute)o));
    	case SECOND: return mapEachOne(a, o -> ex((Second)o));
    	case TIME: return mapEachOne(a, o -> ex((Time)o));
    	case DT: return mapEachOne(a, o -> ex((Dt)o));  
    	}
    	return mapEachOne(a, o -> ex((int) (Integer)o)); 
    }

    @Override public ObjectCol ex(LongCol a) {  
		switch(CType.getType(a.getType())) {
		case TIMESPAN: return mapEachOne(a, o -> ex((Timespan)o));
		case TIMSTAMP: return mapEachOne(a, o -> ex((Timstamp)o));
		}
		return mapEachOne(a, o -> ex((long) (Long)o)); 
	}
    
    @Override public String ex(Op op) { return op.name(); }
    
    private static String intToString(int a, String fallback)     { 
    	return a == SpecialValues.ni ? ""
    		: a == SpecialValues.wi ? "0W"
    		: a == SpecialValues.nwi ? "-0W"
    		: fallback; 
    }

    private static String longToString(long a, String fallback)     { 
    	return a == SpecialValues.ni ? ""
    		: a == SpecialValues.wi ? "0W"
    		: a == SpecialValues.nwi ? "-0W"
    		: fallback; 
    }

    @Override public String ex(Timstamp tm) { return longToString(tm.getLong(), tm.toString()); }
    @Override public String ex(Timespan tm) { return longToString(tm.getLong(), tm.toString()); }
    @Override public String ex(Minute tm) { return intToString(tm.getInt(), tm.toString()); }
    @Override public String ex(Month tm)  { return intToString(tm.getInt(), tm.toString()); }
    @Override public String ex(Second tm) { return intToString(tm.getInt(), tm.toString()); }
    @Override public String ex(Time tm)   { return intToString(tm.getInt(), tm.toString()); }
    @Override public String ex(Dt tm)     { return intToString(tm.getInt(), tm.toString()); }
}
