package com.timestored.jdb.predicate;

import java.sql.Date;
import java.sql.Timestamp;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.timestored.jdb.col.ToFromIntDate;
import com.timestored.jdb.col.ToFromLongTimestamp;

class CastConvertStrategy implements ConvertStrategy {
	
	public static CastConvertStrategy INSTANCE = new CastConvertStrategy();
	
	private CastConvertStrategy() {}
	
	public int toInteger(Object o) {
		if(o instanceof Float) {
			return Ints.saturatedCast(Math.round((float)o));
		} else if(o instanceof Double) {
			return Ints.saturatedCast(Math.round((double)o));
		} else if(o instanceof Long) {
			return Ints.saturatedCast((long) o);
		} else if(o instanceof Byte) {
			return (int) (byte) o;
		} else if(o instanceof Short) {
			return (int) (short) o;
		} else if(o instanceof Integer) {
			return (int) o;
		} else if(o instanceof Date) {
			return ToFromIntDate.INSTANCE.applyAsInt((Date) o);
		}
		throw new IllegalStateException("Unrecognised Type");
	}
	
	public double toDouble(Object o) { 
		if(o instanceof Float) {
			return (double) (float) o;
		} else if(o instanceof Double) {
			return (double) o;
		} else if(o instanceof Long) {
			return (double) (long) o;
		} else if(o instanceof Byte) {
			return (double) (byte) o;
		} else if(o instanceof Short) {
			return (double) (short) o;
		} else if(o instanceof Integer) {
			return (double) (int) o;
		}
		throw new IllegalStateException("Unrecognised Type");
	}
	
	
	public float toFloat(Object o) {
		if(o instanceof Float) {
			return (float) o;
		} else if(o instanceof Double) {
			return (float) (double) o;
		} else if(o instanceof Long) {
			return (float) (long) o;
		} else if(o instanceof Byte) {
			return (float) (byte) o;
		} else if(o instanceof Short) {
			return (float) (short) o;
		} else if(o instanceof Integer) {
			return (float) (int) o;
		}
		throw new IllegalStateException("Unrecognised Type");
	}
	
	
	public long toLong(Object o) { 
		
		if(o instanceof Float) {
			return Math.round((float)o);
		} else if(o instanceof Double) {
			return Math.round((double)o);
		} else if(o instanceof Long) {
			return (long) o;
		} else if(o instanceof Byte) {
			return (long) (byte) o;
		} else if(o instanceof Short) {
			return (long) (short) o;
		} else if(o instanceof Integer) {
			return (long) (int) o;
		} else if(o instanceof Timestamp) {
			return ToFromLongTimestamp.INSTANCE.applyAsLong((Timestamp) o);
		}
		throw new IllegalStateException("Unrecognised Type");
	}
	
	public byte toByte(Object o) { 
		
		if(o instanceof Float) {
			return (byte) (float)o;
		} else if(o instanceof Double) {
			return (byte) (double)o;
		} else if(o instanceof Long) {
			return (byte) (long) o;
		} else if(o instanceof Byte) {
			return (byte) o;
		} else if(o instanceof Short) {
			return (byte) (short) o;
		} else if(o instanceof Integer) {
			return (byte) (int) o;
		}
		throw new IllegalStateException("Unrecognised Type");
	}
	
	public short toShort(Object o) { 
		if(o instanceof Float) {
			return Shorts.saturatedCast((long) (float)o);
		} else if(o instanceof Double) {
			return Shorts.saturatedCast((long) (double)o);
		} else if(o instanceof Long) {
			return Shorts.saturatedCast((long) o);
		} else if(o instanceof Byte) {
			return (short) (byte) o;
		} else if(o instanceof Short) {
			return (short) o;
		} else if(o instanceof Integer) {
			return (short) (int) o;
		}
		throw new IllegalStateException("Unrecognised Type");
	}

	@Override
	public char toCharacter(Object o) {
		char c = ' ';
		if(o instanceof String) {
			String s = (String) o;
			if(s.length() == 1) {
				c = s.charAt(0);
			} else if(s.length() == 0) {
				c = ' ';
			} else {
				throw new IllegalStateException("Cant cast string to char");
			}
		} else {
			c = (char) toShort(o);
		}
		return c;
	}

	@Override
	public String toString(Object o) {
		if(o instanceof String) {
			return (String)o;
		} else if(o instanceof Float) {
			return "" + (float)o;
		} else if(o instanceof Double) {
			return "" + (double)o;
		} else if(o instanceof Long) {
			return "" + (long)o;
		} else if(o instanceof Byte) {
			return "" + (byte)o;
		} else if(o instanceof Short) {
			return "" + (short) o;
		} else if(o instanceof Integer) {
			return "" + (int) o;
		}
		throw new IllegalStateException("Unrecognised Type");
	}

}