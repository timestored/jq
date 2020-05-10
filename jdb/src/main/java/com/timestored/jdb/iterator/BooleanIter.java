/** This code was generated using code generator:com.timestored.jdb.iterator**/
package com.timestored.jdb.iterator;

import java.util.ArrayList;

import com.timestored.jdb.function.ToBooleanFunction;

public interface BooleanIter {

	public static final BooleanIter EMPTY = new EmptyBooleanIter();
	
	int size();
	void reset();
	boolean hasNext();
	boolean nextBoolean();
	
	public static boolean isEquals(BooleanIter a, BooleanIter b) {
		if(a.size() != b.size()) {
			return false;
		}
		while(a.hasNext()) {
			if(a.nextBoolean() != b.nextBoolean()) {
				a.reset();
				b.reset();
				return false;
			}
		}
		a.reset();
		b.reset();
		return true;
	}

	
	public static BooleanIter of(boolean... vals) {
		if(vals.length == 0) {
			return EMPTY;
		}
		ArrayList<Boolean> l = new ArrayList<>(vals.length);
		for(boolean v : vals) {
			l.add(v);
		}
		return wrap(SmartIterator.fromList(l), (Boolean d) -> d);
	}
	
	public static <T> BooleanIter wrap(SmartIterator<T> smartIterator, ToBooleanFunction<T> toBooleanFunction) {
		return new ObjectMappedBooleanInter<T>(smartIterator, toBooleanFunction);
	}
}
