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
public interface Col extends AutoCloseable {
	
	/**
	 * Return the locations of entries that pass a Predicate.
	 * Note if locations contains entries outside the size, sane results are not guaranteed
	 * @throws IOException 
	 */
	Locations select(Locations locations, PredicateFactory predicateFactory);
	Col select(Locations locations);
	Col sort();
	IntegerCol iasc();
	
	int size();
	
	/** @return true if this Col can be appended to using map. **/
	boolean isAppendable();
	
	/** @return true if this Col can be appended to using map then set operations. **/
	boolean isUpdateable();

	/** @return true If this Col is in ascending order **/
	boolean isSorted();

	void setSorted(boolean sorted);

	/**
	 * Check if these values are sorted and if so set the isSorted flag. 
	 * @return true If this Col is in ascending order 
	 **/
	boolean applySorted();
	
	/**
	 * Map the locations to allow get/set access.
	 * If locations lie aboce the current size of the Col, the Col is expanded to that size.
	 */
	void map(Locations locations, RMode rmode) throws IOException;

	short getType();
	void setType(short type);
	
	/** @return the size in bytes of a single entry in this Col **/
	short getSizeInBytes();
	
	/**
	 * Set the size of this column.
	 * If the size has increased, new entries will have undtermined values.
	 * If the size has decreased, entries at the end are chopped off but the memory may not be freed till later.
	 * @throws IOException 
	 */
	void setSize(int newSize) throws IOException;
	
	/**
	 * Note a closed Col, can use map again when it wants.
	 */
	@Override void close() throws Exception;

	void setObject(int i, Object object);
}
