package com.timestored.jdb.database;

import com.google.common.base.Preconditions;

public class Second implements Comparable<Second>,IntegerMappedVal {
	public final int secondsSinceMidnight;

	public Second(int secondsSinceMidnight) { this.secondsSinceMidnight = secondsSinceMidnight; }

	static final String r(int i) { return i<10 ? ("0" + i) : ""+i; }
	
	static String getSpecialIntSt(int s) {
		if(s == SpecialValues.ni) {
			return "0N";
		} else if(s == SpecialValues.nwi) {
			return "-0W";
		} else if(s == SpecialValues.wi) {
			return "0W";
		}
		return null;
	}
	
	public String toString() {
		int i = secondsSinceMidnight;
		String s = getSpecialIntSt(i);
		if(s != null) {
			return s + "v";
		}
		int ai = i >= 0 ? i : -i; 
		return (i >= 0 ? "" : "-") + r(ai / (60*60)) + ":" + r((ai%(60*60)) / 60) + ":" + r(ai%60);
	}

	public boolean equals(final Object o) {
		return (o instanceof Second) ? ((Second) o).secondsSinceMidnight == secondsSinceMidnight : false;
	}

	public int hashCode() { return secondsSinceMidnight; }
	public int compareTo(Second m) { return secondsSinceMidnight - m.secondsSinceMidnight; }
	@Override public int getInt() { return secondsSinceMidnight; }

	public static Second valueOf(String t) {
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
			Preconditions.checkArgument((t.length()==5 && t.charAt(2)==':')
					|| (t.length()==8 && t.charAt(2)==':' && t.charAt(5)==':'));
			int hours = Integer.parseInt(t.substring(0,2));
			int minutes = Integer.parseInt(t.substring(3,5));
			int seconds = t.length()==8 ? Integer.parseInt(t.substring(6,8)) : 0;
			Preconditions.checkArgument(minutes<60);
			Preconditions.checkArgument(seconds<60);
			intVal = mul*((hours*60*60)+(minutes*60)+seconds);
		}
		return new Second(intVal);
	}

	@Override public short getType() { return CType.SECOND.getTypeNum(); }
}