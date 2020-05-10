package com.timestored.jdb.iterator;

import com.google.common.base.Preconditions;

class DoubleIterRange implements DoubleIter {

	private final double lowerBound;
	private final double upperBound;
	private final double step;
	private final int size;
	private double val;

	public DoubleIterRange(double lowerBound, double upperBound, double step) {
		Preconditions.checkArgument(upperBound > lowerBound);
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.step = step;
		
		int steps = 0;
		for(double d = lowerBound; d < upperBound; d += step) {
			steps++;
		}
		this.size = steps;
		this.val = lowerBound;
	}

	@Override public int size() { return size; }
	@Override public void reset() { val = lowerBound; }
	@Override public boolean hasNext() { return val < upperBound; }
	@Override public double nextDouble() { double v = val; val += step; return v; }
	
	@Override public boolean equals(Object obj) {
		// TODO add optimized version to check this specific type
		if(obj instanceof DoubleIter) {
			return DoubleIter.isEquals((DoubleIter) obj, this);
		}
		return false;
	}
}
