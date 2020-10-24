package com.timestored.jdb.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Timestamp;

import com.google.common.base.Objects;
import com.timestored.jdb.function.ToStringFunction;


/**TYPE=TIMSTAMP import com.timestored.jdb.database.Timstamp; **/
/**TYPE=MAPP import com.timestored.jdb.col.Mapp; **/
/**TYPE=COL import com.timestored.jdb.col.Col; **/
/**TYPE=MINUTE import com.timestored.jdb.database.Minute; **/
/**TYPE=SECOND import com.timestored.jdb.database.Second; **/
/**TYPE=TIMESPAN import com.timestored.jdb.database.Timespan; **/
/**TYPE=MONTH import com.timestored.jdb.database.Month; **/
/**TYPE=TIME import com.timestored.jdb.database.Time; **/
/**TYPE=DT import com.timestored.jdb.database.Dt; **/


public interface StringIter {

	public static final StringIter EMPTY = new EmptyStringIter();
	
	int size();
	void reset();
	boolean hasNext();
	String nextString();


	public default ArrayList<String> toList() {
		ArrayList<String> a = new ArrayList<String>(size());
		reset();
		while(hasNext()) {
			a.add(nextString());
		}
		return a;
	}

	public static StringIter of(String... vals) {
		if(vals.length == 0) {
			return EMPTY;
		}
		ArrayList<String> l = new ArrayList<>(vals.length);
		for(String v : vals) {
			l.add(v);
		}
		return wrap(SmartIterator.fromList(l), (String d) -> d);
	}
	
	public static <T> StringIter wrap(SmartIterator<T> smartIterator, ToStringFunction<T> toStringFunction) {
		return new ObjectMappedStringInter<T>(smartIterator, toStringFunction);
	}
	
	public static boolean isEquals(StringIter a, StringIter b) {
		if(a.size() != b.size()) {
			return false;
		}
		while(a.hasNext()) {
			if(!Objects.equal(a.nextString(),b.nextString())) {
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
