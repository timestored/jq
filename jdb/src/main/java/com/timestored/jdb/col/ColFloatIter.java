/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.FloatIter;
import com.timestored.jdb.iterator.Locations;

public class ColFloatIter implements FloatIter {
	
	private final Locations locations;
	private final FloatCol floatCol;
	
	public ColFloatIter(FloatCol floatCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.floatCol = Preconditions.checkNotNull(floatCol);
		locations.reset();
	}
	
	
	@Override public float nextFloat() { return floatCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof FloatIter) {
			return FloatIter.isEquals((FloatIter) obj, this);
		}
		return false;
	}
}