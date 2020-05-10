package com.timestored.jdb.col;

import java.sql.Date;
import java.util.TimeZone;

import com.timestored.jdb.database.Consts;

public class ToFromIntDate implements ToFromInt<Date> {

	/** 10957 days between 2000.01.01 - 1970.01.01 **/
	private static long DATE_GAP = 1000l*60*60*24 * 10957l;
	private static final TimeZone TZ = TimeZone.getDefault();
	public static final ToFromIntDate INSTANCE = new ToFromIntDate();
	
	private ToFromIntDate() { }
	
	@Override public int applyAsInt(Date d) {
		if(d == null) {
			return Consts.Nulls.INTEGER;
		}
		long l = d.getTime();
		if(l == Consts.Nulls.LONG) {
			return Consts.Nulls.INTEGER;
		}
		long millis = l + TZ.getOffset(l) - DATE_GAP;
		return (int)(millis / 86400000L);
	}
	
	@Override public Date apply(int v) {
		if(v == Consts.Nulls.INTEGER) {
			return new Date(Consts.Nulls.LONG);
		}
		long millis = DATE_GAP + 86400000L * v;
		return new Date(millis);
	}
}
