/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.LongIter;
import com.timestored.jdb.iterator.Locations;

public class ColLongIter implements LongIter {
	
	private final Locations locations;
	private final LongCol longCol;
	
	public ColLongIter(LongCol longCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.longCol = Preconditions.checkNotNull(longCol);
		locations.reset();
	}
	
	
	@Override public long nextLong() { return longCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof LongIter) {
			return LongIter.isEquals((LongIter) obj, this);
		}
		return false;
	}
}