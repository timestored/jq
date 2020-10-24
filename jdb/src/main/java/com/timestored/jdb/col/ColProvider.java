package com.timestored.jdb.col;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.kexception.NYIException;

import lombok.ToString;

public abstract class ColProvider {

//	public abstract BooleanCol createBooleanCol(String identifier) throws IOException;
	public abstract IntegerCol createIntegerCol(String identifier) throws IOException;
	public abstract DoubleCol createDoubleCol(String identifier) throws IOException;
	public abstract FloatCol createFloatCol(String identifier) throws IOException;
	public abstract LongCol createLongCol(String identifier) throws IOException;
	public abstract StringCol createStringCol(String identifier) throws IOException;
	public abstract ByteCol createByteCol(String identifier) throws IOException;
	public abstract ShortCol createShortCol(String identifier) throws IOException;
	public abstract CharacterCol createCharacterCol(String identifier) throws IOException;
	public abstract ObjectCol createObjectCol(String identifier) throws IOException;
	

	public static ColProvider getInMemory() {
		return new InMemoryColProvider();
	}

	public static ColProvider getDisk(File folder) {
		return new DiskColProvider(folder);
	}

	public static final ObjectCol emptyObjectCol = new MemoryObjectCol(0);

	public static final BooleanCol emptyBooleanCol = new MemoryBooleanCol(0);
	private static final ByteCol emptyByteCol = new MemoryByteCol(0);
	private static final ShortCol emptyShortCol = new MemoryShortCol(0);
	private static final IntegerCol emptyIntegerCol = new MemoryIntegerCol(0);
	public static final LongCol emptyLongCol = new MemoryLongCol(0);
	private static final FloatCol emptyFloatCol = new MemoryFloatCol(0);
	private static final DoubleCol emptyDoubleCol = new MemoryDoubleCol(0);
	public static final CharacterCol emptyCharacterCol = new MemoryCharacterCol(0);
	public static final StringCol emptyStringCol = new MemoryStringCol(0);
	private static final LongCol emptyTimstampCol = new MemoryLongCol(0);
	private static final LongCol emptyTimespanCol = new MemoryLongCol(0);
	private static final IntegerCol emptyDateCol = new MemoryIntegerCol(0);
	private static final IntegerCol emptySecondCol = new MemoryIntegerCol(0);
	private static final IntegerCol emptyMinuteCol = new MemoryIntegerCol(0);
	private static final IntegerCol emptyTimeCol = new MemoryIntegerCol(0);
	
	static {
		emptySecondCol.setType((short) -CType.SECOND.getTypeNum());
		emptyMinuteCol.setType((short) -CType.MINUTE.getTypeNum());
		emptyTimeCol.setType((short) -CType.TIME.getTypeNum());
		emptyTimstampCol.setType((short) -CType.TIMSTAMP.getTypeNum());
		emptyTimespanCol.setType((short) -CType.TIMESPAN.getTypeNum());
		emptyDateCol.setType((short) -CType.DT.getTypeNum());
		
		if(Database.QCOMPATIBLE) {
			emptyBooleanCol.setSorted(false);
			emptyByteCol.setSorted(false);
			emptyShortCol.setSorted(false);
			emptyIntegerCol.setSorted(false);
			emptyLongCol.setSorted(false);
			emptyFloatCol.setSorted(false);
			emptyDoubleCol.setSorted(false);
			emptyCharacterCol.setSorted(false);
			emptyStringCol.setSorted(false);
			emptyTimstampCol.setSorted(false);
			emptyTimespanCol.setSorted(false);
			emptyDateCol.setSorted(false);
			emptySecondCol.setSorted(false);
			emptyMinuteCol.setSorted(false);
			emptyTimeCol.setSorted(false);
		}
	}
	
	public static Col emptyCol(CType ctype) { return emptyCol(ctype.getTypeNum()); }
	
	public static Col emptyCol(short typeNum) {
		Col c = null;
		switch (Math.abs(typeNum)) {
		case 0: c = emptyObjectCol;  break;
		case 1: c = emptyBooleanCol;  break;
		case 4: c = emptyByteCol;  break;
		case 5: c = emptyShortCol;  break;
		case 6: c = emptyIntegerCol;  break;
		case 7: c = emptyLongCol;  break;
		case 8: c = emptyFloatCol;  break;
		case 9: c = emptyDoubleCol;  break;
		case 10: c = emptyCharacterCol;  break;
		case 11: c = emptyStringCol;  break;
		case 12: c = emptyTimstampCol;  break;
		case 14: c = emptyDateCol;  break;
		case 16: c = emptyTimespanCol;  break;
		case 17: c = emptyMinuteCol;  break;
		case 18: c = emptySecondCol;  break;
		case 19: c = emptyTimeCol;  break;
		default:
		}
		if(c!=null) {
			if(Database.QCOMPATIBLE) {
				c.setSorted(false);
			}
			// TODO hacky until we have proper memory management
			try {
				c.setSize(0);
			} catch (IOException e) {
				c = ColProvider.getInMemory(CType.getType(typeNum), 0);
			}
			return c;
		}
		throw new UnsupportedOperationException();
	}

	@ToString
	private static class InMemoryColProvider extends ColProvider {
		public ObjectCol createObjectCol(String identifier) { return new MemoryObjectCol(); }
		public BooleanCol createBooleanCol(String identifier) { return new MemoryBooleanCol(); }
		public IntegerCol createIntegerCol(String identifier) { return new MemoryIntegerCol(); }
		public DoubleCol createDoubleCol(String identifier) { return new MemoryDoubleCol(); }
		public FloatCol createFloatCol(String identifier) { return new MemoryFloatCol(); }
		public LongCol createLongCol(String identifier) { return new MemoryLongCol(); }
		public StringCol createStringCol(String identifier) { return new MemoryStringCol(5); }
		public ByteCol createByteCol(String identifier) { return new MemoryByteCol(); } 
		public ShortCol createShortCol(String identifier) { return new MemoryShortCol(); } 
		public CharacterCol createCharacterCol(String identifier) { return new MemoryCharacterCol(); } 
	}

	public static MemoryStringCol toStringCol(Collection<String> ls) {
		MemoryStringCol vals = new MemoryStringCol(ls.size());
		try {
			vals.setSize(ls.size());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		int i = 0;
		for(String s : ls) {
			vals.set(i++, s);
		}
		return vals;
	}
	
	public static MemoryStringCol toStringCol(String[] list) {
		return toStringCol(Arrays.asList(list));
	}
	
	public static CharacterCol toCharacterCol(String vals) {
		return new MemoryCharacterCol(vals.toCharArray());
	}
	
	public static ObjectCol toCharacterCol(String[] vals) {
		ObjectCol oc = new MemoryObjectCol(vals.length);
		for(int i=0; i<vals.length; i++) {
			oc.set(i, toCharacterCol(vals[i]));
		}
		return oc;
	}
	public static Col getInMemory(CType cType, int initialSize) {
		switch(cType) {
		case BOOLEAN:	return new MemoryBooleanCol(initialSize);
		case BYTE:	return new MemoryByteCol(initialSize);
		case CHARACTER:	return new MemoryCharacterCol(initialSize);
		case DOUBLE:	return new MemoryDoubleCol(initialSize);
		case FLOAT:	return new MemoryFloatCol(initialSize);
		case INTEGER:	return new MemoryIntegerCol(initialSize);
		case LONG:	return new MemoryLongCol(initialSize);
		case SHORT:	return new MemoryShortCol(initialSize);
		case STRING:	return new MemoryStringCol(initialSize);
		case TIME:	return new MemoryIntegerCol(initialSize);
		case DT:
		case MINUTE:	
		case OBJECT:	
		case SECOND:		
		case MONTH:	
			MemoryIntegerCol mi = new MemoryIntegerCol(initialSize);
			mi.setType(cType.getTypeNum());
			return mi;
		case TIMESPAN:
		case TIMSTAMP:
			MemoryLongCol mj = new MemoryLongCol(initialSize);
			mj.setType(cType.getTypeNum());
			return mj;
		case MAPP:
		}
		throw new NYIException("can't create that type: " + cType);
	}
	
	public static ObjectCol toCharacterCol(List<String> a) {
		return toCharacterCol(a.toArray(new String[0]));
	}
	
	
	
	

	
    public static ObjectCol o(int initialSize, Function<Integer,Object> f) {
    	ObjectCol r = new MemoryObjectCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
	/*
	 * The below code is copy pasted from ColCreator
	 * 
	 * The below code is copy pasted from ColCreator
	 * 
	 * The below code is copy pasted from ColCreator
	 * 
	 * The below code is copy pasted from ColCreator
	 * 
	 */

	
    public static ShortCol h(int initialSize, Function<Integer,Short> f) {
    	ShortCol r = new MemoryShortCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static ShortCol h(int initialSize, short constant) {
    	return h(initialSize, i -> constant); 
    }
    
    
    public static ShortCol h() { return new MemoryShortCol(0); }
	
    public static StringCol s(int initialSize, Function<Integer,String> f) {
    	StringCol r = new MemoryStringCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static StringCol s(int initialSize, String constant) {
    	return s(initialSize, i -> constant); 
    }
    
    
    public static StringCol s() { return new MemoryStringCol(0); }
	
    public static CharacterCol c(int initialSize, Function<Integer,Character> f) {
    	CharacterCol r = new MemoryCharacterCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static CharacterCol c(int initialSize, char constant) {
    	return c(initialSize, i -> constant); 
    }
    
    
    public static CharacterCol c() { return new MemoryCharacterCol(0); }
	

    public static LongCol j(List<Long> longVals) {
    	return j(longVals.size(), i -> longVals.get(i));
    }
    
    public static LongCol j(int initialSize, Function<Integer,Long> f) {
    	LongCol r = new MemoryLongCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static LongCol j(int initialSize, long constant) {
    	return initialSize == 0 ? emptyLongCol : j(initialSize, i -> constant); 
    }
    
    
    public static LongCol j() { return new MemoryLongCol(0); }
	
    public static DoubleCol f(int initialSize, Function<Integer,Double> f) {
    	DoubleCol r = new MemoryDoubleCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static DoubleCol f(int initialSize, double constant) {
    	return f(initialSize, i -> constant); 
    }
    
    
    public static DoubleCol f() { return new MemoryDoubleCol(0); }
	
    public static FloatCol e(int initialSize, Function<Integer,Float> f) {
    	FloatCol r = new MemoryFloatCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static FloatCol e(int initialSize, float constant) {
    	return e(initialSize, i -> constant); 
    }
    
    
    public static FloatCol e() { return new MemoryFloatCol(0); }
	
    public static IntegerCol i(int initialSize, Function<Integer,Integer> f) {
    	IntegerCol r = new MemoryIntegerCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static IntegerCol i(int initialSize, int constant) {
    	return i(initialSize, i -> constant); 
    }
    
    
    public static IntegerCol i() { return new MemoryIntegerCol(0); }
	
    public static ByteCol x(int initialSize, Function<Integer,Byte> f) {
    	ByteCol r = new MemoryByteCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static ByteCol x(int initialSize, byte constant) {
    	return x(initialSize, i -> constant); 
    }
    
    
    public static ByteCol x() { return new MemoryByteCol(0); }
	
    public static BooleanCol b(int initialSize, Function<Integer,Boolean> f) {
    	BooleanCol r = new MemoryBooleanCol(initialSize);
    	for(int i=0; i<initialSize; i++) {
    		r.set(i, f.apply(i));
    	}
    	return r; 
    }
    
    public static BooleanCol b(int initialSize, boolean constant) {
    	return b(initialSize, i -> constant); 
    }
    
    
    public static BooleanCol b() { return new MemoryBooleanCol(0); }
}
