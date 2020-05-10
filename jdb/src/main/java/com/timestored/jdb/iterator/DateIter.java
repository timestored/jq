/** This code was generated using code generator:com.timestored.jdb.iterator**/
package com.timestored.jdb.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Date;
import java.sql.Timestamp;

import com.google.common.base.Objects;
import com.timestored.jdb.function.ToDateFunction;

public interface DateIter {

	public static final DateIter EMPTY = new EmptyDateIter();
	
	int size();
	void reset();
	boolean hasNext();
	Date nextDate();


	public default ArrayList<Date> toList() {
		ArrayList<Date> a = new ArrayList<Date>(size());
		reset();
		while(hasNext()) {
			a.add(nextDate());
		}
		return a;
	}

	public static DateIter of(Date... vals) {
		if(vals.length == 0) {
			return EMPTY;
		}
		ArrayList<Date> l = new ArrayList<>(vals.length);
		for(Date v : vals) {
			l.add(v);
		}
		return wrap(SmartIterator.fromList(l), (Date d) -> d);
	}
	
	public static <T> DateIter wrap(SmartIterator<T> smartIterator, ToDateFunction<T> toDateFunction) {
		return new ObjectMappedDateInter<T>(smartIterator, toDateFunction);
	}
	
	public static boolean isEquals(DateIter a, DateIter b) {
		if(a.size() != b.size()) {
			return false;
		}
		while(a.hasNext()) {
			if(!Objects.equal(a.nextDate(),b.nextDate())) {
				a.reset();
				b.reset();
				return false;
			}
		}
		a.reset();
		b.reset();
		return true;
	}
}
