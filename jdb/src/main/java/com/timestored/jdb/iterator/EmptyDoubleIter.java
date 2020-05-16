package com.timestored.jdb.iterator;
/**TYPE=TIMSTAMP import com.timestored.jdb.database.Timstamp; **/
/**TYPE=DATE import java.sql.Date; **/
/**TYPE=MAPP import com.timestored.jdb.col.Mapp; **/
/**TYPE=COL import com.timestored.jdb.col.Col; **/
/**TYPE=MINUTE import com.timestored.jdb.database.Minute; **/
/**TYPE=SECOND import com.timestored.jdb.database.Second; **/
/**TYPE=TIMESPAN import com.timestored.jdb.database.Timespan; **/
/**TYPE=MONTH import com.timestored.jdb.database.Month; **/
/**TYPE=TIME import com.timestored.jdb.database.Time; **/
/**TYPE=DT import com.timestored.jdb.database.Dt; **/

class EmptyDoubleIter implements DoubleIter {
	@Override public int size() { return 0; }
	@Override public void reset() { }
	@Override public double nextDouble() { throw new IllegalStateException(); }
	@Override public boolean hasNext() { return false; }
	@Override public String toString() { return "EmptyDoubleIter"; }
	@Override public boolean equals(Object obj) {
		if(obj instanceof EmptyDoubleIter) {
			return true;
		}
		if(obj instanceof DoubleIter) {
			return DoubleIter.isEquals((DoubleIter) obj, this);
		}
		return false;
	}
}
