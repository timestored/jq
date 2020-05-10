package com.timestored.jdb.database;

import com.google.common.base.Preconditions;

public class Minute implements Comparable<Minute>,IntegerMappedVal {
	public int minutesSinceMidnight;

	public Minute(int minutesSinceMidnight) { this.minutesSinceMidnight = minutesSinceMidnight; }

	private final String s(int i) { return i<10 ? ("0" + i) : ""+i; }
	
	public String toString() {
		int i = minutesSinceMidnight;
		String s = Second.getSpecialIntSt(i);
		if(s != null) {
			return s + "u";
		}
		int ai = i >= 0 ? i : -i;
		return(i >= 0 ? "" : "-") + s(ai / 60) + ":" + s(ai % 60);
	}

	public boolean equals(final Object o) {
		return (o instanceof Minute) ? ((Minute) o).minutesSinceMidnight == minutesSinceMidnight : false;
	}

	public int hashCode() { return minutesSinceMidnight; }
	public int compareTo(Minute m) { return minutesSinceMidnight - m.minutesSinceMidnight; }
	@Override public int getInt() { return minutesSinceMidnight; }

	public static Minute valueOf(String t) {
		t = t.trim().toLowerCase();
		int intVal = SpecialValues.ni;
		if(t.equals("0w")) {
			intVal = SpecialValues.wi;
		} else if(t.equals("-0w")) {
			intVal = SpecialValues.nwi;
		} else if(t.equals("0n")) {
			intVal = SpecialValues.ni;
		} else {
			int mul = 1;
			if(t.charAt(0) == '-') {
				mul = -1;
				t = t.substring(1);
			}
			Preconditions.checkArgument(t.length()>=5 && t.charAt(2)==':');
			int hours = Integer.parseInt(t.substring(0,2));
			int minutes = Integer.parseInt(t.substring(3,5));
			Preconditions.checkArgument(minutes<60);
			intVal = mul*((hours*60)+minutes);
		}
		return new Minute(intVal);
		
	}

	@Override public short getType() { return CType.MINUTE.getTypeNum(); }
}