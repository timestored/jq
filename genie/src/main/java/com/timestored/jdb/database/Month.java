package com.timestored.jdb.database;

import java.text.DecimalFormat;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Month implements Comparable<Month>,IntegerMappedVal {
	private final int monthsSince2000;

	public String toString() {
		int i = monthsSince2000;
		String s = Second.getSpecialIntSt(i);
		if(s != null) {
			return s + "m";
		}
		int y = 2000+(i/12);
		int m = 1+(i%12);
		if(i < 0) {
			y = y-1;
			m = 12 - m;
		}
		DecimalFormat dfy = new DecimalFormat("0000");
		return dfy.format(y) + "." + (m < 10 ? "0"+m : m);
	}
	

	public static Month valueOf(String t) {
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
			Preconditions.checkArgument(t.length()==7 && t.charAt(4)=='.');
			int year = Integer.parseInt(t.substring(0,4));
			int month = -1+Integer.parseInt(t.substring(5,7));
			Preconditions.checkArgument(month<12 && month>=0);
			intVal = mul*(((year-2000)*12)+month);
		}
		return new Month(intVal);
	}
	
	public boolean equals(final Object o) {
		return (o instanceof Month) ? ((Month) o).monthsSince2000 == monthsSince2000 : false;
	}

	public int hashCode() { return monthsSince2000; }
	public int compareTo(Month m) { return monthsSince2000 - m.monthsSince2000; }
	@Override public int getInt() { return monthsSince2000; }
	
	@Override public short getType() { return 0; }
}
