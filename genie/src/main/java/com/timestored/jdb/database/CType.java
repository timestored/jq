package com.timestored.jdb.database;

import static com.timestored.jdb.database.SpecialValues.*;

import java.sql.Time;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum CType implements CTypeI {

	OBJECT((short)0, (short)0, "Object", "Object", Object[].class, java.sql.Types.JAVA_OBJECT, 
			'*', null, null, null),
	BOOLEAN((short) -1, (short)1, "Boolean", "boolean", Boolean.class, java.sql.Types.BOOLEAN, 
			'b', null, null, null),
	BYTE((short)-4, (short)1, "Byte", "byte", Byte.class, java.sql.Types.BINARY, 
			'x', new Byte((byte) 0), null, null),
	SHORT((short)-5, (short)2, "Short", "short", Short.class, java.sql.Types.SMALLINT, 
			'h', new Short(Short.MIN_VALUE), new Short((short)-(1+Short.MIN_VALUE)), new Short((short)(1+Short.MIN_VALUE))), 
	INTEGER((short)-6, (short)4, "Integer", "int", Integer.class, java.sql.Types.INTEGER,
			'i', new Integer(ni), new Integer(wi), new Integer(nwi)),
	LONG((short)-7, (short)8, "Long", "long", Long.class, java.sql.Types.BIGINT,
			'j', new Long(nj), new Long(wj), new Long(nwj)),
	FLOAT((short)-8, (short)4, "Float", "float", Float.class, java.sql.Types.REAL,
			'e', new Float(ne), new Float(we), new Float(nwe)),
	DOUBLE((short)-9, (short)8, "Double", "double", Double.class, java.sql.Types.DOUBLE,
			'f', new Double(nf), new Double(wf), new Double(nwf)),
	CHARACTER((short)-10, (short)8, "Character", "char", Character.class, java.sql.Types.NVARCHAR,
			'c', new Character(' '), null, null),
	STRING((short)-11, (short)4, "String", "String", String.class, java.sql.Types.VARCHAR,
			's', "", null, null),
	TIMSTAMP((short)-12, (short)8, "Timstamp", "Timstamp", Timstamp.class, java.sql.Types.TIMESTAMP,
			'p', new Timstamp(nj), new Timstamp(wj), new Timstamp(nwj)),
	MONTH((short)-13, (short)4, "Month", "Month", Month.class, java.sql.Types.TIMESTAMP_WITH_TIMEZONE,
			'm', new Integer(ni), new Integer(wi), new Integer(nwi)),
	DT((short)-14, (short)4, "Dt", "Dt", Dt.class, java.sql.Types.DATE,
			'd', new Timstamp(nj), new Timstamp(wj), new Timstamp(nwj)),
	TIMESPAN((short)-16, (short)8, "Timespan", "Timespan", Timespan.class, java.sql.Types.TIME_WITH_TIMEZONE, // WRONG
			'n', new Timstamp(nj), new Timstamp(wj), new Timstamp(nwj)),
	MINUTE((short)-17, (short)4, "Minute", "Minute", Minute.class, java.sql.Types.DISTINCT,
			'u', new Integer(ni), new Integer(wi), new Integer(nwi)),
	SECOND((short)-18, (short)4, "Second", "Second", Second.class, java.sql.Types.STRUCT,
			'v', new Integer(ni), new Integer(wi), new Integer(nwi)),
	TIME((short)-19, (short)4, "Time", "Time", Time.class, java.sql.Types.TIME,
			't', new Integer(ni), new Integer(wi), new Integer(nwi)),
	MAPP((short)99, (short)0, "Mapp", "Mapp", Object.class, java.sql.Types.OTHER);
	
	@Getter private final short typeNum;
	@Getter private final short sizeInBytes;
	@Getter private final String longJavaName;
	@Getter private final String nativeJavaName;
	@Getter private final Class<?> javaClass;
	@Getter private final int sqlTypeNum;
	@Getter private final char characterCode;
	@Getter private final Object nullValue;
	@Getter private final Object posInfinity;
	@Getter private final Object negInfinity;

	CType(short typeNum, short sizeInBytes, String longJavaName, String nativeJavaName, Class<?> javaClass, int sqlTypeNum) {
		this.typeNum = typeNum;
		this.sizeInBytes = sizeInBytes;
		this.longJavaName = longJavaName;
		this.nativeJavaName = nativeJavaName;
		this.javaClass = javaClass;
		this.sqlTypeNum = sqlTypeNum;
		this.characterCode = ' ';
		this.nullValue = null;
		this.posInfinity = null;
		this.negInfinity = null;
	}

	private static final Collection<CType> BUILTIN_TYPES = Sets.immutableEnumSet(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, CHARACTER );

	private static final Map<Class<?>, CType> LOOKUP = 
			Maps.uniqueIndex(Arrays.asList(CType.values()), CType::getJavaClass);    
	private static final Map<Short, CType> TYPE_LOOKUP = 
			Maps.uniqueIndex(Arrays.asList(CType.values()), CType::getTypeNum);  
	private static final Map<Integer, CType> SQL_TYPE_LOOKUP = 
			Maps.uniqueIndex(Arrays.asList(CType.values()), CType::getSqlTypeNum);    
	private static final Map<Character, CType> CHAR_LOOKUP = 
			Maps.uniqueIndex(Arrays.asList(CType.values()), CType::getCharacterCode);   

	@Nullable public static CType fromJavaClass(Class<?> javaClass) {
        return LOOKUP.get(javaClass);
    }

	@Nullable public static CType fromJavaClass(byte typeNum) {
        return TYPE_LOOKUP.get(typeNum);
    }

	@Nullable public static CType fromSqlType(int sqlTypeNum) {
        return SQL_TYPE_LOOKUP.get(sqlTypeNum);
    }
	
	public static Collection<CType> builtinTypes() { return BUILTIN_TYPES; }
	public static Collection<CType> allTypes() { return Arrays.asList(CType.values()); }

	public static final Collection<CTypeI> NUMERIC_TYPES = new HashSet<>();
	public static final Collection<CTypeI> ALL_NATIVE_TYPES = new HashSet<>();
	static {
		NUMERIC_TYPES.addAll(Arrays.asList(BOOLEAN, SHORT, INTEGER, LONG, FLOAT, DOUBLE));
		ALL_NATIVE_TYPES.addAll(NUMERIC_TYPES);
		ALL_NATIVE_TYPES.add(BYTE);
		ALL_NATIVE_TYPES.add(CHARACTER);
		ALL_NATIVE_TYPES.add(STRING);
		for(CTypeI d : NUMERIC_TYPES) {
			ALL_NATIVE_TYPES.add(new ListWrapper(d));
		}
		ALL_NATIVE_TYPES.add(new ListWrapper(BYTE));
		ALL_NATIVE_TYPES.add(new ListWrapper(CHARACTER));
		ALL_NATIVE_TYPES.add(new ListWrapper(STRING));
		ALL_NATIVE_TYPES.add(new ListWrapper(OBJECT));
	}
	
	public CTypeI getListType() {
		if(getTypeNum()>0) {
			return this;
		}
		return new ListWrapper(this);
	}

	private static class ListWrapper implements CTypeI {
		private final CTypeI dt;
		public ListWrapper(CTypeI dt) { this.dt = dt; }
		@Override public char getCharacterCode() { return dt.getCharacterCode(); }
		@Override public short getTypeNum() { return (short) (-1 * dt.getTypeNum()); }
		@Override public String getLongJavaName() { return dt.getLongJavaName() + "Col"; }
		@Override public String getNativeJavaName() { return dt.getLongJavaName() + "Col"; }
		@Override public CTypeI getListType() { return this; }
		@Override public String getQName() { return dt.getQName(); }
		@Override public String toString() { return "ListOf" + dt.toString();
		}
	}

	/** For a given class return the KdbType or null if none apply */
	public static CTypeI getType(Class<?> clas) {
		if(clas!=null) {
			for(CType kt : values()) {
				if(kt.javaClass.equals(clas)) {
					return kt;
				}
			}
		}
		throw new IllegalArgumentException();
	}

	/** For a given class return the KdbType or null if none apply */
	public static CType getType(char typeChar) {
		return CHAR_LOOKUP.get(typeChar);
	}

	/** For a given class return the KdbType or null if none apply */
	public static CType getType(short typeNum) {
		return TYPE_LOOKUP.get(typeNum > 0 ? (short) -typeNum : typeNum);
	}


	/** For a given class return the KdbType or null if none apply */
	public static CType getType(String typeName) {
		if(typeName.isEmpty()) {
			return CType.STRING;
		}
		for(CType kt : values()) {
			if(kt.getQName().equalsIgnoreCase(typeName)) {
				return kt;
			}
		}
		throw new IllegalArgumentException();
	}
	
	/** @return true if the object o is a null in KDB. */
	public static boolean isNull(Object o) {
		if(o!=null) {
			if(o instanceof Double) {
				return Double.isNaN((double) o);
			} else if(o instanceof Float) {
				return Float.isNaN((float) o);
			}
			for(CType kt : values()) {
				if(o.equals(kt.nullValue)) {
					return true;
				}
			}
		}
		return false;
	}

	
	/** @return true if the object o is a positive infinity in KDB.  */
	public static boolean isPositiveInfinity(Object o) {
		if(o!=null) {
			if(o instanceof Double) {
				double d = (double) o;
				return Double.isInfinite(d) && d > 0;
			} else if(o instanceof Float) {
				float d = (float) o;
				return Float.isInfinite(d) && d > 0;
			}
			for(CType kt : values()) {
				if(o.equals(kt.posInfinity)) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	public String getQName() {
		String n = this.name();
		switch(n){
			case "DT": return "date";
			case "FLOAT": return "real";
			case "DOUBLE": return "float";
			case "CHARACTER": return "char";
			case "INTEGER": return "int";
			case "STRING": return "symbol";
			case "TIMSTAMP": return "timestamp";
		}
		return n.toLowerCase();
	}
	
	/** @return true if the object o is a negative infinity in KDB. */
	public static boolean isNegativeInfinity(Object o) {// these classes don't have a proper equals so need checked manually
		if(o!=null) {
			if(o instanceof Double) {
				double d = (double) o;
				return Double.isInfinite(d) && d < 0;
			} else if(o instanceof Float) {
				float d = (float) o;
				return Float.isInfinite(d) && d < 0;
			}
			for(CType kt : values()) {
				if(o.equals(kt.negInfinity)) {
					return true;
				}
			}
		}
		return false;
	}
}
