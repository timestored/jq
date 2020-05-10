/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.ShortIter;
import com.timestored.jdb.iterator.Locations;

public class ColShortIter implements ShortIter {
	
	private final Locations locations;
	private final ShortCol shortCol;
	
	public ColShortIter(ShortCol shortCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.shortCol = Preconditions.checkNotNull(shortCol);
		locations.reset();
	}
	
	
	@Override public short nextShort() { return shortCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof ShortIter) {
			return ShortIter.isEquals((ShortIter) obj, this);
		}
		return false;
	}
}