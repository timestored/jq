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
import com.timestored.jdb.function.ToDoubleFunction;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ObjectMappedDoubleInter<T> implements DoubleIter {

	private final SmartIterator<T> smartIterator;
	private final ToDoubleFunction<T> toDoubleFunction;

	@Override public double nextDouble() { return toDoubleFunction.applyAsDouble(smartIterator.next()); }
	@Override public boolean hasNext() { return smartIterator.hasNext(); }
	@Override public int size() { return smartIterator.size(); }
	@Override public void reset() { smartIterator.reset(); }

	@Override public boolean equals(Object obj) {
		if(obj instanceof DoubleIter) {
			return DoubleIter.isEquals((DoubleIter) obj, this);
		}
		return false;
	}
}