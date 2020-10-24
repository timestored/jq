package com.timestored.jdb.database;

public class Consts {

	public static class Mins {
		public static final boolean BOOLEAN = false;
		public static final byte BYTE = Byte.MIN_VALUE;
		public static final short SHORT = Short.MIN_VALUE;
		public static final int INTEGER = SpecialValues.nwi;
		public static final long LONG = SpecialValues.nwj;
		public static final float FLOAT = SpecialValues.nwe;
		public static final double DOUBLE = SpecialValues.nwf;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MIN_VALUE;
	}
	

	public static class Maxs {
		public static final boolean BOOLEAN = true;
		public static final byte BYTE = Byte.MAX_VALUE;
		public static final short SHORT = Short.MAX_VALUE;
		public static final int INTEGER = SpecialValues.wi;
		public static final long LONG = SpecialValues.wj;
		public static final float FLOAT = SpecialValues.we;
		public static final double DOUBLE = SpecialValues.wf;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MAX_VALUE;
	}
	

	public static class Nulls {
		public static final boolean BOOLEAN = false;
		public static final byte BYTE = Byte.MIN_VALUE;
		public static final short SHORT = Short.MIN_VALUE;
		public static final int INTEGER = SpecialValues.ni;
		public static final long LONG = SpecialValues.nj;
		public static final float FLOAT = SpecialValues.ne;
		public static final double DOUBLE = SpecialValues.nf;
		public static final long TIMESTAMP = LONG;
		public static final char CHARACTER = Character.MIN_VALUE;
	}
	
}
