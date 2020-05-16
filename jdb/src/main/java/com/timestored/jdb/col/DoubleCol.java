package com.timestored.jdb.col;

import java.io.IOException;

/**TYPE=TIMSTAMP import com.timestored.jdb.database.Timstamp; **/
/**TYPE=DATE import java.sql.Date; **/
/**TYPE=MAPP import com.timestored.jdb.col.Mapp; **/
/**TYPE=MINUTE import com.timestored.jdb.database.Minute; **/
/**TYPE=SECOND import com.timestored.jdb.database.Second; **/
/**TYPE=TIMESPAN import com.timestored.jdb.database.Timespan; **/
/**TYPE=MONTH import com.timestored.jdb.database.Month; **/
/**TYPE=TIME import com.timestored.jdb.database.Time; **/
import com.timestored.jdb.database.CType;
import com.timestored.jdb.function.DiadToDoubleFunction;
import com.timestored.jdb.function.DoublePredicate;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.function.MonadToDoubleFunction;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;

public interface DoubleCol extends Col {

	DoubleIter select();
	DoubleCol select(Locations locations);
	Locations select(Locations locations, DoublePredicate doublePredicate);
	
	boolean addAll(DoubleIter doubleIterator) throws IOException;
	boolean addAll(DoubleCol doubleCol) throws IOException;

	DoubleCol sort();
	
	void set(int i, double val);
	double get(int i);
	
	void setType(short type);
	@Override default short getType() { return CType.DOUBLE.getTypeNum(); }
	@Override default short getSizeInBytes() { return CType.DOUBLE.getSizeInBytes(); }
	

	public static boolean isEquals(DoubleCol a, DoubleCol b) {
		if(a.size() != b.size()) {
			return false;
		}
		return a.getType()==b.getType() && DoubleIter.isEquals(a.select(), b.select());
	}

	double max();
	double min();
	double first();
	double last();
	
	boolean contains(DoubleCol needle);
	boolean contains(double needle);
	
	IntegerCol find(DoubleCol needle);
	int find(double needle);
	
	/** 
	 *  @return The highest index at which a value <= val can be found. 
	 *  The result is -1 if val is less than the first element.
	 *  i.e. Given a sorted list this finds where the value should be placed.
	 **/
	int bin(double val);

	/** @return The first index in this col which is >= val **/
	int binr(double val);
	

	default Double[] toDoubleArray() throws IOException {
		Double[] r = new Double[this.size()];
		DoubleIter si = select();
		int i = 0;
		while(si.hasNext()) {
			r[i++] = si.nextDouble();
		}
		return r;
	}

	DoubleCol map(DoubleCol b, DiadToDoubleFunction f);
	
	DoubleCol map(MonadToDoubleFunction f);
	public default DoubleCol map(double d) {
		return map((a) -> d);
	}
}

