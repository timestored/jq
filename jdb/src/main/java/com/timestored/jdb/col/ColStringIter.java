/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.StringIter;
import com.timestored.jdb.iterator.Locations;

public class ColStringIter implements StringIter {
	
	private final Locations locations;
	private final StringCol StringCol;
	
	public ColStringIter(StringCol StringCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.StringCol = Preconditions.checkNotNull(StringCol);
		locations.reset();
	}
	
	
	@Override public String nextString() { return StringCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof StringIter) {
			return StringIter.isEquals((StringIter) obj, this);
		}
		return false;
	}
}