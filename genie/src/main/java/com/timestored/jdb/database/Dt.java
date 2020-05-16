package com.timestored.jdb.database;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import lombok.AllArgsConstructor;

/** Thin wrapper around int so that when passing an int it has a "type" **/
@AllArgsConstructor
public class Dt implements IntegerMappedVal {
	/** 10957 days between 2000.01.01 - 1970.01.01 **/
	private static long EPOCH_GAP = 1000l*60*60*24 * 10957l;
	private static final TimeZone TZ = TimeZone.getDefault();
	private final int daysSince2000;
	private final static LocalDate ORIGIN = LocalDate.of(2000, java.time.Month.JANUARY, 01);
	private static final SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy.MM.dd");
	
	public static Dt valueOf(String dtString) {
		return new Dt(asInt(dtString));
	}

	
	public static int asInt(String dtString) {
		String t = dtString.trim().toLowerCase();
		switch (t) {
		case "0w": return SpecialValues.wi;
		case "-0w": return SpecialValues.nwi;
		case "0n": return SpecialValues.ni;
		}
		try {
			if(t.length() >= 7 && t.charAt(4) == '-' && t.charAt(7) == '-') {
				t = t.replaceAll("-", ".");
			}
			java.sql.Date sqlDate = new java.sql.Date(dtFormat.parse(t).getTime());
			long l = sqlDate.getTime();
			if(l == SpecialValues.nj) {
				return SpecialValues.ni;
			}
			long millis = l + TZ.getOffset(l) - EPOCH_GAP;
			return (int)(millis / 86400000L);
		} catch (ParseException e) { 
			throw new NumberFormatException(e.getLocalizedMessage());
		}
	}
	
	public Date toDate() { 
		if(daysSince2000 == SpecialValues.ni) {
			return new Date(SpecialValues.nj);
		}
		long millis = EPOCH_GAP + 86400000L * daysSince2000;
		return new Date(millis); 
	} 
	
	@Override public String toString() {
		return daysSince2000 == SpecialValues.ni ? "0Nd"
				: daysSince2000 == SpecialValues.wi ? "0Wd"
				: daysSince2000 == SpecialValues.nwi ? "-0Wd"
				: dtFormat.format(toDate());
	}

	public static Dt fromLocalDate(LocalDate ld) {
		return new Dt((int) ChronoUnit.DAYS.between(ORIGIN, ld));
	}

	@Override public int getInt() { return daysSince2000; }

	@Override public short getType() { return CType.DT.getTypeNum(); }

}
