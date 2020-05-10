package com.timestored.jdb.database;

import lombok.AllArgsConstructor;

/** Thin wrapper around long so that when passing an long it has a "type" **/
@AllArgsConstructor
public class Timstamp implements Comparable<Timstamp>,LongMappedVal {
	public final long nanosSinceMillenium;
	private static final long NAN = 1_000_000_000;
	private static final long HOUR = 60*60*NAN;
	private static final long DAY = 24*HOUR;

	public String toString() {
		String s = Timespan.getSpecialLongSt(nanosSinceMillenium);
		if (s != null) {
			return s;
		}
		long aj = Math.abs(nanosSinceMillenium);
		int day = ((int) (nanosSinceMillenium / DAY));
		long time = aj % DAY;
		if(nanosSinceMillenium < 0) {
			time = DAY - time;
			day = (int) (((nanosSinceMillenium+1)/DAY) - 1);
		}
		String a = new Dt(day).toString() + "D" + Timespan.toTimeString(time);
		return a;
	}
	
	public static Timstamp valueOf(String dtString) {
		String t = dtString.trim().toLowerCase();
		long longVal = SpecialValues.nj;
		if(t.equals("0w")) {
			longVal = SpecialValues.wj;
		} else if(t.equals("-0w")) {
			longVal = SpecialValues.nwj;
		} else if(t.equals("0n")) {
			longVal = SpecialValues.nj;
		} else {
			int days = 0;
			int p = t.indexOf('d');
			if(p != -1) {
				String dPart = t.substring(0, p);
				days = Dt.valueOf(dPart).getInt();
				t = t.substring(p+1);
			}
			longVal = ((days*DAY) + Timespan.getTimeNanos(t));
		}
		return new Timstamp(longVal);
	}

	static final String r(long i) { return i<10 ? ("0" + i) : ""+i; }
	
	public Second getSecond() {
		int secondsSinceMidnight = (int) ((nanosSinceMillenium % DAY) / NAN);
		return new Second(secondsSinceMidnight);
	}

	public int compareTo(Timstamp t) {
		return nanosSinceMillenium > t.nanosSinceMillenium ? 1 : nanosSinceMillenium < t.nanosSinceMillenium ? -1 : 0;
	}

	public boolean equals(final Object o) {
		return (o instanceof Timstamp) ? ((Timstamp) o).nanosSinceMillenium == nanosSinceMillenium : false;
	}

	public int hashCode() {
		return (int) (nanosSinceMillenium ^ (nanosSinceMillenium >>> 32));
	}

	@Override public long getLong() { return nanosSinceMillenium; }

	@Override public short getType() { return CType.TIMSTAMP.getTypeNum(); }

}
