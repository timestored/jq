package com.timestored.jdb.iterator;

import com.google.common.base.Preconditions;

class EmptyLocations implements Locations {

	@Override public int size() { return 0; }
	@Override public void reset() { }
	@Override public int nextInteger() { return 0; }
	@Override public boolean hasNext() { return false; }
	@Override public Locations first(int n) { return this; }
	@Override public Locations last(int n) { return this; }
	@Override public int getMin() { return Integer.MAX_VALUE; }
	@Override public int getMax() { return Integer.MIN_VALUE; }
	@Override public int get(int idx) { throw new IllegalStateException(); }
	@Override public String toString() { return "EmptyLocations"; }
	@Override public boolean isEmpty() { return true; }
	
	@Override public boolean equals(Object obj) {
		if(obj instanceof EmptyLocations) {
			return true;
		}
		if(obj instanceof Locations) {
			return Locations.isEquals((Locations) obj, this);
		}
		return false;
	}
	@Override public Locations setBounds(int lowerBound, int upperBound) {
		Preconditions.checkArgument(lowerBound ==0 && upperBound == 0);
		return this; 
	}
}
