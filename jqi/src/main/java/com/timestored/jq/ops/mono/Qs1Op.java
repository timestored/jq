package com.timestored.jq.ops.mono;

import java.util.function.Function;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryCharacterCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jq.Context;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.Op;

/** String is the basically reverse of {@link ParseOp} except nulls are "" **/
public class Qs1Op extends MonadReduceToObject {
	public static Qs1Op B3 = new Qs1Op();
	@Override public String name() { return ".Q.s1"; }

    public String asString(Object o) {
		return (String) super.run(o);
	}
    
	@Override public Object run(Object o) {
		Object r = asString(o);
		if(r instanceof String) {
			return ColProvider.toCharacterCol(((String) r));	
		}
		return r;
	}
	
    public String ex(String a)  { return "`" + StringOp.INSTANCE.ex(a); }
    public String ex(byte a)    { return "0x" + StringOp.INSTANCE.ex(a); }
    public String ex(boolean a) { return StringOp.INSTANCE.ex(a) + "b"; }
    public String ex(char a)    { return "\""+a+"\""; }
    public String ex(short a)   { return (a == SpecialValues.nh ? "0N" : StringOp.INSTANCE.ex(a)) + "h"; }
    public String ex(int a)     { return (a == SpecialValues.ni ? "0N" : StringOp.INSTANCE.ex(a)) + "i"; }
    public String ex(long a)    { return (a == SpecialValues.nj ? "0N" : StringOp.INSTANCE.ex(a)); }
    public String ex(float a)   { return (Float.isNaN(a) ? "0N" : StringOp.INSTANCE.ex(a)) + "e"; }
    
    public String ex(double a)  {
    	if(Double.isNaN(a)) {
    		return "0n";
    	}
    	String s = StringOp.INSTANCE.ex(a);
    	return s.contains(".") || s.contains("w") ? s : s + "f"; 
    }
    
    @Override public String ex(Month tm)  { return intToString(tm.getInt(), StringOp.INSTANCE.ex(tm)+"m", 'm'); }
    @Override public String ex(Timespan tm) { return longToString(tm.getLong(), StringOp.INSTANCE.ex(tm), 'n'); }
    @Override public String ex(Timstamp tm) { return longToString(tm.getLong(), StringOp.INSTANCE.ex(tm), 'p'); }
    @Override public String ex(Minute tm) { return intToString(tm.getInt(), StringOp.INSTANCE.ex(tm), 'u'); }
    @Override public String ex(Second tm) { return intToString(tm.getInt(), StringOp.INSTANCE.ex(tm), 'v'); }
    @Override public String ex(Time tm)   { return intToString(tm.getInt(), StringOp.INSTANCE.ex(tm), 't'); }
    @Override public String ex(Dt tm)     { return intToString(tm.getInt(), StringOp.INSTANCE.ex(tm), 'd'); }

    private static String intToString(int a, String fallback, char typeLetter)     { 
    	return a == SpecialValues.ni ? ("0N"  + typeLetter)
    		: a == SpecialValues.wi ?  ("0W"  + typeLetter)
    		: a == SpecialValues.nwi ? ("-0W" + typeLetter)
    		: fallback; 
    }

    private static String longToString(long a, String fallback, char typeLetter)     { 
    	return a == SpecialValues.nj ? ("0N"  + typeLetter)
    		: a == SpecialValues.wj ?  ("0W"  + typeLetter)
    		: a == SpecialValues.nwj ? ("-0W" + typeLetter)
    		: fallback; 
    }
    
    @Override public Object ex(ObjectCol c) { 
    	StringBuilder sb = new StringBuilder();
		if(c.size() > 1) {	sb.append("("); }
    	if(c.size() == 0) { return "()"; }
		if(c.size() == 1) {	sb.append(","); }
    	sb.append(this.asString(IndexOp.INSTANCE.run(c, 0)));
    	for(int i=1; i<c.size(); i++) {
    		String s = this.asString(IndexOp.INSTANCE.run(c, i));
    		sb.append(";").append(s);
    	}
    	if(c.size() > 1) {	sb.append(")"); }
    	return sb.toString();
    }
    


	private String mapEachOne(Col c, String spacer) {
    	return mapEachOne(c, spacer, "", "");
    }
    
    private String mapEachOne(Col c, String spacer, String pre, String post) {
    	StringBuilder sb = new StringBuilder();
		CType cType = CType.getType(c.getType());
		String att = c.isSorted() ? "`s#" : "";
    	if(c.size() == 0) {
    		if(cType.equals(CType.CHARACTER)) {
    			return att + "\\\"\\\"";
    		}
    		return att + (c.getType() != 0 ? "`" + cType.getQName() + "$": "") + "()";
    	}
    	sb.append(att);
		if(c.size() == 1) {
			sb.append(",");
		}
    	sb.append(pre);
		String s = StringOp.INSTANCE.asString(IndexOp.INSTANCE.run(c, 0));
		sb.append(replaceOddities(cType, s));
    	for(int i=1; i<c.size(); i++) {
    		s = StringOp.INSTANCE.asString(IndexOp.INSTANCE.run(c, i));
    		sb.append(spacer).append(replaceOddities(cType, s));
    	}
    	sb.append(post);
    	switch(cType) {
			case BYTE:
			case LONG:
			case DT:
			case OBJECT:
			case CHARACTER:
			case STRING:
			case TIMESPAN:
			case TIMSTAMP:
    			break;
			case DOUBLE:
				if(sb.indexOf(".") == -1 && sb.indexOf("n") == -1 && sb.indexOf("w") == -1) { sb.append(cType.getCharacterCode()); }
				break;
			case TIME:
			case MINUTE:
			case SECOND:
				if(sb.indexOf(":") == -1) { sb.append(cType.getCharacterCode()); }
				break;
    		default:
    			sb.append(cType.getCharacterCode());
    	}
    	return sb.toString(); 
    }

	private static String replaceOddities(CType cType, String s) {
		if(s.length() == 0) {
			s = cType.equals(CType.DOUBLE) ? "0n" : cType.equals(CType.STRING) ? "" : "0N";
		}
		return s;
	}
    @Override public String ex(ByteCol a){ return mapEachOne(a, "", "0x", ""); }
    @Override public String ex(BooleanCol a){ return mapEachOne(a, ""); }
    @Override public String ex(ShortCol a)  { return mapEachOne(a, " "); }
    @Override public String ex(IntegerCol a){ return mapEachOne(a, " "); }
    @Override public String ex(LongCol a)   { return mapEachOne(a, " "); }
    @Override public String ex(FloatCol a)  { return mapEachOne(a, " "); }
    @Override public String ex(DoubleCol a) { return mapEachOne(a, " "); }
    @Override public String ex(CharacterCol a) { return mapEachOne(a, "", "\"", "\""); }
    @Override public String ex(StringCol a) { 	return mapEachOne(a, "`", "`", ""); }
    
    @Override public String ex(Op op) { return op.name(); }
    
    @Override public Object ex(Mapp o) {
    	Col k = o.getKey();
    	String b = (k.size() < 2 ? "(" : "");
    	String a = (k.size() < 2 ? ")" : "");
		return (b + asString(k) + a) + "!" + asString(o.getValue());
	}
    
    @Override public Object ex(Tbl o) {
		return "+" + ex((Mapp) o);
	}
}
