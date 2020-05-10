/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.IntegerIter;
import com.timestored.jdb.iterator.Locations;

public class ColIntegerIter implements IntegerIter {
	
	private final Locations locations;
	private final IntegerCol intCol;
	
	public ColIntegerIter(IntegerCol intCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.intCol = Preconditions.checkNotNull(intCol);
		locations.reset();
	}
	
	
	@Override public int nextInteger() { return intCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof IntegerIter) {
			return IntegerIter.isEquals((IntegerIter) obj, this);
		}
		return false;
	}
}