package com.timestored.jdb.iterator;

import java.util.ArrayList;

import com.carrotsearch.hppc.IntArrayList;
import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.MemoryBooleanCol;

public interface Locations extends IntegerIter {
	
	public static final Locations EMPTY = new EmptyLocations();
	
	/** Modify these locations to select only the top n **/
	Locations first(int n);
	
	/** Modify these locations to select only the bottom n **/
	Locations last(int n);

	int get(int idx);
	int getMin();
	int getMax();
	
	/** 
	 * Given that Locations represent locations (p1, p2, p3..., pn) 
	 * setBounds trims the locations to between (pLowerBound,px+1,...pUpperBound)
	 * subsequent calls will again restrict the bounds. This is an optimised way to
	 * handle the common issue where predicates such as less than or greater than only 
	 * continuous blocks of locations to match.
	 * @return The locations with the relvant bounds.
	 */
	Locations setBounds(int lowerBound, int upperBound);
	
	
	public static Locations upTo(int upperBound) {
		if(upperBound == 0) {
			return EMPTY;
		}
		return new RangeLocations(upperBound);
	}
	
	public static Locations forRange(int lowerBound, int upperBound) {
		if(upperBound == lowerBound) {
			return EMPTY;
		}
		return new RangeLocations(lowerBound, upperBound);
	}
	
	public static Locations wrap(IntArrayList intArrayList) {
		if(intArrayList.isEmpty()) {
			return EMPTY;
		}
		return new IntArrayListLocations(intArrayList);
	}

	public static Locations of(int... vals) {
		if(vals.length == 0) {
			return EMPTY;
		}
		IntArrayList l = new IntArrayList(vals.length);
		for(int v : vals) {
			l.add(v);
		}
		return wrap(l);
	}
	
	default boolean isEmpty() { return size() == 0; }


	public static boolean isEquals(Locations a, Locations b) {
		return IntegerIter.isEquals(a, b);
	}

	public default BooleanCol toBooleanCol() {
		int m = getMax();
		MemoryBooleanCol mcb = new MemoryBooleanCol(m+1);
		this.reset();
		while(hasNext()) {
			mcb.set(nextInteger(), true);
		}
		return mcb;
	}
}
