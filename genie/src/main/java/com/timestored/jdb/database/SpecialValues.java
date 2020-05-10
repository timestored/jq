package com.timestored.jdb.database;

public class SpecialValues {



	/*
	 * hacky method of getting shortcodes for static final variables
	 */
	public static final Object NULL_Object = new Object[] {};

	public static final boolean nb = false; // Doesn't really exist
	public static final byte nx = 0x00; // Doesn't really exist
	
    public static final short nh = Short.MIN_VALUE;
    public static final short nwh = (short) ((short) nh + 1);
    public static final short wh = Short.MAX_VALUE;

    public static final int ni = Integer.MIN_VALUE;
    public static final int nwi = ni + 1;
    public static final int wi = Integer.MAX_VALUE;

    public static final long nj = Long.MIN_VALUE;
    public static final long nwj = nj + 1;
    public static final long wj = Long.MAX_VALUE;

    public static final float ne=Float.NaN;
    public static final float we=Float.POSITIVE_INFINITY;
    public static final float nwe=Float.NEGATIVE_INFINITY;

    public static final double nf=Double.NaN;
    public static final double wf=Double.POSITIVE_INFINITY;
    public static final double nwf=Double.NEGATIVE_INFINITY;

    public static final char nc = ' ';
    public static final String ns = "";
	public static final Timespan nn = new Timespan(SpecialValues.nj);
	public static final Timstamp np = new Timstamp(SpecialValues.nj);
	public static final Month nm = new Month(SpecialValues.ni);
	public static final Dt nd = new Dt(SpecialValues.ni);
	public static final Minute nu = new Minute(SpecialValues.ni);
	public static final Second nv = new Second(SpecialValues.ni);
	public static final Time nt = new Time(SpecialValues.ni);

    public static boolean isNull(boolean a) { return false; }
    public static boolean isNull(short a) { return a == nh; }
    public static boolean isNull(int a) { return a == ni; }
    public static boolean isNull(long a) { return a == nj; }
    public static boolean isNull(float a) { return Float.isNaN(a); }
    public static boolean isNull(double a) { return Double.isNaN(a); }
}
