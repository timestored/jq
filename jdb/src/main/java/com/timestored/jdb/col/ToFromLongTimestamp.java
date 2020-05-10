package com.timestored.jdb.col;

import java.sql.Timestamp;
import java.util.TimeZone;

import com.timestored.jdb.database.Consts;

public class ToFromLongTimestamp implements ToFromLong<Timestamp> {

	/** 10957 days between 2000.01.01 - 1970.01.01 **/
	private static long DATE_GAP = 1000l*60*60*24 * 10957l;
	private static final TimeZone TZ = TimeZone.getDefault();
	public static final ToFromLongTimestamp INSTANCE = new ToFromLongTimestamp();
	
	private ToFromLongTimestamp() { }
	
	@Override public long applyAsLong(Timestamp v) {
		if(v == null) {
			return Consts.Nulls.LONG;
		}
		long j = v.getTime();
		if(j == Consts.Nulls.LONG) {
			return j;
		}
		// offset to q
		long millis = j + TZ.getOffset(j) - DATE_GAP;
		return (1000000 * millis) + (v.getNanos() % 1000000);
	}
	
	@Override public Timestamp apply(long v) {
		final long N = 1000000000;
		long d = v < 0 ? ((v + 1) / N - 1) : v/N;
		if(v == Consts.Nulls.LONG) {
			return new Timestamp(v);
		}
		long millis = DATE_GAP + 1000 * d;
		// offset from q
		millis = millis - TZ.getOffset(millis - TZ.getOffset(millis));
		Timestamp p = new Timestamp(millis);
		p.setNanos((int) (v - N * d));
		return p;
	}

}
