package com.timestored.jdb.resultset;

import java.sql.ResultSet;


/**
 * Basically a standard {@link ResultSet} but with keyedColumns and a table caption added on.
 * The caption is useful for displaying extra on on the Object that the result set may be based on.
 * The keyedColumns are good for displaying Maps as results sets. 
 */
public interface KeyedResultSet extends ResultSet {
	
	/**
	 * @return The number of key columns in this resultset.
	 * This will always be >=0 and less than the total number of columns.
	 */
	public int getNumberOfKeyColumns();
	
	/**
	 * @return Extra detail about the ResultSet
	 * i.e. if it is based on a map/Object/Collection and what types are in it.
	 */
	public String getCaption();
}
