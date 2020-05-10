package com.timestored.jdb.database;

public class Consts {

	public static class Mins {
		public static final boolean BOOLEAN = false;
		public static final byte BYTE = Byte.MIN_VALUE;
		public static final short SHORT = Short.MIN_VALUE;
		public static final int INTEGER = Integer.MIN_VALUE;
		public static final long LONG = Long.MIN_VALUE;
		public static final float FLOAT = Float.NEGATIVE_INFINITY;
		public static final double DOUBLE = Double.NEGATIVE_INFINITY;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MIN_VALUE;
	}
	

	public static class Maxs {
		public static final boolean BOOLEAN = true;
		public static final byte BYTE = Byte.MAX_VALUE;
		public static final short SHORT = Short.MAX_VALUE;
		public static final int INTEGER = Integer.MAX_VALUE;
		public static final long LONG = Long.MAX_VALUE;
		public static final float FLOAT = Float.POSITIVE_INFINITY;
		public static final double DOUBLE = Double.POSITIVE_INFINITY;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MAX_VALUE;
	}
	

	public static class Nulls {
		public static final boolean BOOLEAN = false;
		public static final byte BYTE = Byte.MIN_VALUE;
		public static final short SHORT = Short.MIN_VALUE;
		public static final int INTEGER = Integer.MIN_VALUE;
		public static final long LONG = Long.MIN_VALUE;
		public static final float FLOAT = Float.NaN;
		public static final double DOUBLE = Double.NaN;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MIN_VALUE;
	}
	
}
