package com.timestored.jdb.col;

import com.timestored.jdb.iterator.Locations;

/**
 * Represents a vector of data all of the same type.
 * Access is either via:
 * 1. {@link #map(Locations, RMode)} then get/set operations.
 * 2. Selects with predicates where the map is handled for you. 
 */
public interface Tbl extends Mapp {

	StringCol getKey();
	ObjectCol getValue();
	
	public default Col getCol(String name) {
		int p = getKey().find(name);
		ObjectCol v = getValue();
		if(p >= 0 && p<v.size()) {
			return (Col) v.get(p);
		}
		return null;
	}
	
}
