package com.timestored.jdb.iterator;


import com.google.common.base.Preconditions;
import com.timestored.jdb.col.DoubleCol;

/**TYPE=TIMSTAMP import com.timestored.jdb.database.Timstamp; **/
/**TYPE=DATE import java.sql.Date; **/
/**TYPE=MAPP import com.timestored.jdb.col.Mapp; **/
/**TYPE=COL import com.timestored.jdb.col.Col; **/
/**TYPE=MINUTE import com.timestored.jdb.database.Minute; **/
/**TYPE=SECOND import com.timestored.jdb.database.Second; **/
/**TYPE=TIMESPAN import com.timestored.jdb.database.Timespan; **/
/**TYPE=MONTH import com.timestored.jdb.database.Month; **/
/**TYPE=TIME import com.timestored.jdb.database.Time; **/

public class ColDoubleIter implements DoubleIter {
	
	private final Locations locations;
	private final DoubleCol doubleCol;
	
	public ColDoubleIter(DoubleCol doubleCol, Locations locations) { 
		this.locations = Preconditions.checkNotNull(locations);
		this.doubleCol = Preconditions.checkNotNull(doubleCol);
		locations.reset();
	}
	
	
	@Override public double nextDouble() { return doubleCol.get(locations.nextInteger());}
	@Override public boolean hasNext() { return locations.hasNext();}
	@Override public int size() { return locations.size(); }
	@Override public void reset() { locations.reset(); }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof DoubleIter) {
			return DoubleIter.isEquals((DoubleIter) obj, this);
		}
		return false;
	}
}