package com.timestored.jdb.col;

import java.io.IOException;

import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.predicate.PredicateFactory;

/**
 * Represents a vector of data all of the same type.
 * Access is either via:
 * 1. {@link #map(Locations, RMode)} then get/set operations.
 * 2. Selects with predicates where the map is handled for you. 
 */
public interface Mapp extends Col,AutoCloseable {

	Col getKey();
	Col getValue();
	
	public default boolean isKeyedTable() {
		return getKey() instanceof Tbl && getValue() instanceof Tbl; 
	}
}
