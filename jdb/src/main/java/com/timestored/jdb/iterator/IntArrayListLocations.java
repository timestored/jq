package com.timestored.jdb.iterator;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Preconditions;

import lombok.ToString;

@ToString
class IntArrayListLocations implements Locations {

	private final IntArrayList intArrayList;
	private int size = 0;
	private int lowerBound = 0;
	private int upperBound = 0;
	private int p = 0;
	
	IntArrayListLocations(IntArrayList intArrayList) { 
		this.intArrayList = intArrayList;
		this.size = intArrayList.size();
		this.upperBound = size;
	}
	
	@Override public boolean hasNext() { return p < upperBound; }
	@Override public int size() { return size; }
	@Override public int nextInteger() {
		int v = p<intArrayList.size() ? intArrayList.get(p) : 0;
		p++;
		return v;
	}
	@Override public void reset() { p = lowerBound;  }

	@Override public Locations first(int n) {
		if(n < size) {
			setBounds(0, n);
		}
		this.p = lowerBound;
		return this;
	}
	

	public Locations setBounds(int lowerBound, int upperBound) {
		Preconditions.checkArgument(upperBound >= lowerBound && lowerBound >= 0);
		this.lowerBound = this.lowerBound + lowerBound; 
		this.size = upperBound - lowerBound;
		this.upperBound = this.lowerBound + size;
		this.p = this.lowerBound; 
		return this;
	}

	@Override public Locations last(int n) {
		if(n < size) {
			setBounds(size - n, size);
		}
		this.p = lowerBound;
		return this;
	}

	@Override public int get(int idx) { return intArrayList.get(idx); }
	@Override public int getMin() { return size > 0 ? intArrayList.get(lowerBound) : Integer.MAX_VALUE; }
	@Override public int getMax() { return size > 0 ? intArrayList.get(size - 1) : Integer.MIN_VALUE; }
	

	@Override public boolean equals(Object obj) {
		if(obj instanceof Locations) {
			return Locations.isEquals((Locations) obj, this);
		}
		return false;
	}
}