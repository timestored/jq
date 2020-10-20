package com.timestored.jdb.col;

import java.io.IOException;
import java.util.function.Function;

import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;

public class ProjectedDoubleCol extends BaseDoubleCol {

	private int size;
	private final Function<Integer,Double> f;

	public ProjectedDoubleCol(int size, short type, Function<Integer,Double> f) {
		this.size = size;
		setType(type);
		this.f = f;
	}

	@Override public DoubleCol sort() {
		if(!isSorted()) {
			MemoryDoubleCol r = new MemoryDoubleCol(this);
			return r.sort();
		}
		return this;
	}
	
	@Override public IntegerCol iasc() {
		MemoryDoubleCol r = new MemoryDoubleCol(this);
		return r.iasc();
	}

	@Override public double get(int i) { return f.apply(i); }
	@Override public int size() { return size; }
	@Override public boolean isAppendable() { return false; }
	@Override public boolean isUpdateable() { return false; }
	@Override public void map(Locations locations, RMode rmode) throws IOException { }
	@Override public void setSize(int newSize) throws IOException { this.size = newSize;  }
	@Override public void close() throws Exception { }
	@Override boolean uncheckedAddAll(DoubleIter myDoubleIterator) throws IOException {
		throw new UnsupportedOperationException();
	}
	@Override void uncheckedSet(int index, double value) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override public IntegerCol find(DoubleCol needle) {
		return new MemoryDoubleCol(this).find(needle);
	}
    
}
