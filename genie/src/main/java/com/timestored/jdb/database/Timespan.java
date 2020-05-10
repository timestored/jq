package com.timestored.jdb.database;

import java.text.DecimalFormat;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;

/** Thin wrapper around long so that when passing an long it has a "type" **/
@AllArgsConstructor
public class Timespan implements Comparable<Timespan>,LongMappedVal {
	public final long nanosSinceMidnight;
	private static final long NAN = 1_000_000_000;
	private static final long HOUR = 60*60*NAN;
	public static final long DAY = 24*HOUR;

	public String toString() {
		String s = getSpecialLongSt(nanosSinceMidnight);
		if (s != null) {
			return s;
		}
		s = nanosSinceMidnight < 0 ? "-" : "";
		long aj = Math.abs(nanosSinceMidnight);
		int day = ((int) (aj / DAY));
		s += day + "D";
		s += toTimeString(aj);
		return s;
	}

	public static String toTimeString(long nanoSecondTime) {
		String f = r(((nanoSecondTime % DAY) / HOUR)) + ":"; 
		f += r(((nanoSecondTime % HOUR) / (60*NAN)));
		f += ":" + r(((nanoSecondTime % (60*NAN)) / NAN));
		f += "." + new DecimalFormat("000000000").format((nanoSecondTime % NAN));
		return f;
	}
	
	public static Timespan valueOf(String dtString) {
		String t = dtString.trim().toLowerCase();
		long longVal = SpecialValues.nj;
		if(t.equals("0w")) {
			longVal = SpecialValues.wj;
		} else if(t.equals("-0w")) {
			longVal = SpecialValues.nwj;
		} else if(t.equals("0n")) {
			longVal = SpecialValues.nj;
		} else {
			int mul = 1;
			if(t.charAt(0) == '-') {
				mul = -1;
				t = t.substring(1);
			}
			int days = 0;
			if(dtString.contains("d")) {
				int p = t.indexOf('d');
				days = Integer.parseInt(t.substring(0, p));
				t = t.substring(p+1);
			}
			longVal = mul*((days*DAY) + getTimeNanos(t));
		}
		return new Timespan(longVal);
	}

	/**
	 * @param t  Time to possibly nanosecond precision e.g. 01:34:67.123456789
	 */
	static long getTimeNanos(String t) {
		int seconds = 0;
		long nanos = 0;
		Preconditions.checkArgument(t.length() == 0 || t.length() >= 5);
		
		if(t.length() >= 5) {
			Preconditions.checkArgument(t.charAt(2) == ':');
			int dotPos = t.indexOf(".");
			String secs = dotPos != -1 ? t.substring(0, dotPos) : t;
			seconds = Second.valueOf(secs).getInt();
			if(dotPos > 0 && dotPos < t.length()-1) {
				String nans = t.substring(dotPos+1);
				nans = nans.substring(0, Math.min(nans.length(), 9));
				nans = Strings.padEnd(nans, 9, '0').substring(0, 9); 
				nanos = Long.parseLong(nans);
			}
		}
		return (seconds*NAN)+nanos;
	}

	static final String r(long i) { return i<10 ? ("0" + i) : ""+i; }
	
	public Second getSecond() {
		int secondsSinceMidnight = (int) ((nanosSinceMidnight % DAY) / NAN);
		return new Second(secondsSinceMidnight);
	}

	static String getSpecialLongSt(long s) {
		if(s == SpecialValues.nj) {
			return "0N";
		} else if(s == SpecialValues.nwj) {
			return "-0W";
		} else if(s == SpecialValues.wj) {
			return "0W";
		}
		return null;
	}
	
	public int compareTo(Timespan t) {
		return nanosSinceMidnight > t.nanosSinceMidnight ? 1 : nanosSinceMidnight < t.nanosSinceMidnight ? -1 : 0;
	}

	public boolean equals(final Object o) {
		return (o instanceof Timespan) ? ((Timespan) o).nanosSinceMidnight == nanosSinceMidnight : false;
	}

	public int hashCode() {
		return (int) (nanosSinceMidnight ^ (nanosSinceMidnight >>> 32));
	}

	@Override public long getLong() { return nanosSinceMidnight; }

	@Override public short getType() { return CType.TIMESPAN.getTypeNum(); }

}
