/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;


import com.google.common.base.Preconditions;
import com.timestored.jdb.iterator.ByteIter;
import com.timestored.jdb.iterator.Locations;

public class ColByteIter implements ByteIter {
	
	private final Locations locations;
	private final ByteCol byteCol;
	
	public ColByteIter(ByteCol byteCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.byteCol = Preconditions.checkNotNull(byteCol);
		locations.reset();
	}
	
	
	@Override public byte nextByte() { return byteCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof ByteIter) {
			return ByteIter.isEquals((ByteIter) obj, this);
		}
		return false;
	}
}