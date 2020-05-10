package com.timestored.jdb.iterator;

import java.util.ArrayList;
import java.util.Arrays;

import com.carrotsearch.hppc.DoubleArrayList;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.function.ToDoubleFunction;

public interface DoubleIter {

	public static final DoubleIter EMPTY = new EmptyDoubleIter();
	
	int size();
	void reset();
	boolean hasNext();
	double nextDouble();


	public default DoubleArrayList toList() {
		DoubleArrayList a = new DoubleArrayList(size());
		reset();
		while(hasNext()) {
			a.add(nextDouble());
		}
		return a;
	}

	public static DoubleIter upTo(double upperBound) {
		if(upperBound == 0) {
			return EMPTY;
		}
		return new DoubleIterRange((double) 0, upperBound, (double) 1);
	}
	
	
	public static DoubleIter forRange(double lowerBound, double upperBound, double step) {
		if(upperBound == lowerBound) {
			return EMPTY;
		}
		return new DoubleIterRange(lowerBound, upperBound, step);
	}
	
	public static String toString(DoubleIter res) {
		DoubleArrayList matches = res.toList();
		return Arrays.toString(matches.toArray());
	}

	
	public static boolean isEquals(DoubleIter a, DoubleIter b) {
		if(a.size() != b.size()) {
			return false;
		}
		while(a.hasNext()) {
			double va = a.nextDouble();
			double vb = b.nextDouble();
			if(va != vb && !(CType.isNull(va) && CType.isNull(vb))) {
				a.reset();
				b.reset();
				return false;
			}
		}
		a.reset();
		b.reset();
		return true;
	}

	
	public static DoubleIter of(double... vals) {
		if(vals.length == 0) {
			return EMPTY;
		}
		ArrayList<Double> l = new ArrayList<>(vals.length);
		for(double v : vals) {
			l.add(v);
		}
		return wrap(SmartIterator.fromList(l), (Double d) -> d);
	}
	
	public static <T> DoubleIter wrap(SmartIterator<T> smartIterator, ToDoubleFunction<T> toDoubleFunction) {
		return new ObjectMappedDoubleInter<T>(smartIterator, toDoubleFunction);
	}
}
