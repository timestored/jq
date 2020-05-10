package com.timestored.jdb.database;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;

/** Thin wrapper around int so that when passing an int it has a "type" **/
@AllArgsConstructor
public class Time implements Comparable<Time>,IntegerMappedVal {
	private final int millisSinceMidnight;
	
	@Override public String toString() {
		long m = millisSinceMidnight;
		LocalTime v = LocalTime.MIN.plusNanos((m > 0 ? m : -m) * 1_000_000);
		return (m >= 0 ? "" : "-") + v.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
	}

	public boolean equals(final Object o) {
		return (o instanceof Time) ? ((Time) o).millisSinceMidnight == millisSinceMidnight : false;
	}

	public int hashCode() { return millisSinceMidnight; }
	public int compareTo(Time m) { return millisSinceMidnight - m.millisSinceMidnight; }
	@Override public int getInt() { return millisSinceMidnight; }

	public static Time valueOf(String t) {
		t = t.trim().toLowerCase();
		int intVal = SpecialValues.ni;
		if(t.equals("0w")) {
			intVal = SpecialValues.wi;
		} else if(t.equals("-0w")) {
			intVal = SpecialValues.nwi;
		} else if(t.equals("0n")) {
			intVal = SpecialValues.ni;
		} else {
			// 01:34:67.9012	
			int mul = 1;
			if(t.charAt(0) == '-') {
				mul = -1;
				t = t.substring(1);
			}
			int millis = 0;
			int seconds = 0;
			if(t.length()>8) {
				Preconditions.checkArgument(t.length()>=9 && t.charAt(8) == '.');
				seconds = Second.valueOf(t.substring(0, 8)).getInt();
				String m = t.substring(9);
				millis += m.length()>0 ? 100*Integer.parseInt(""+m.charAt(0)) : 0;
				millis += m.length()>1 ? 10*Integer.parseInt(""+m.charAt(1)) : 0;
				millis += m.length()>2 ? Integer.parseInt(""+m.charAt(2)) : 0;
			} else {
				seconds = Second.valueOf(t).getInt();
			}
			intVal = mul*((seconds*1000)+millis);
		}
		return new Time(intVal);
	}

	public static Object fromLocalTime(LocalTime lt) {
		int h = lt.getHour() * 60 * 60 * 1000;
		int m = lt.getMinute() * 60 * 1000;
		int s = lt.getSecond() * 1000;
		int n = lt.getNano();
		int millis = (n - (n%1_000_000))/1_000_000;
		return new Time(h + m + s + millis);
	}

	@Override public short getType() { return CType.TIME.getTypeNum(); }
}
