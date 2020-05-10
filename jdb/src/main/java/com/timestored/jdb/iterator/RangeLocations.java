package com.timestored.jdb.iterator;

import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.ToString;

/**
 * Locations based on an integer range. 
 */
@ToString
public class RangeLocations implements Locations {
	
	@Getter private int lowerBound;
	@Getter private int upperBound;
	private int p;
	private int size;

	public RangeLocations(int upperBound) { this(0, upperBound); }
	
	public RangeLocations(int lowerBound, int upperBound) { 
		setBounds(lowerBound, upperBound);
		p = lowerBound;
	}

	public Locations setBounds(int lowerBound, int upperBound) {
		Preconditions.checkArgument(upperBound >= lowerBound && lowerBound >= 0);
		this.lowerBound = this.lowerBound + lowerBound; 
		this.upperBound = this.lowerBound + (upperBound - lowerBound); 
		this.size = upperBound - lowerBound;
		this.p = this.lowerBound;
		return this;
	}

	@Override public int nextInteger() { return p++; }
	@Override public int get(int idx) { return lowerBound + idx; }
	@Override public void reset() { p = lowerBound; }
	@Override public boolean hasNext() { return p < upperBound; }
	@Override public int size() { return size; }

	@Override public Locations first(int n) {
		if(n < size) {
			setBounds(0, n);
		}
		p = lowerBound;
		return this;
	}

	@Override public Locations last(int n) {
		if(n < size) {
			setBounds(size - n, size);
		}
		p = lowerBound;
		return this;
	}

	@Override public int getMin() { return size > 0 ? lowerBound : Integer.MAX_VALUE; }
	@Override public int getMax() { return size > 0 ? upperBound - 1 : Integer.MIN_VALUE; }
	

	@Override public boolean equals(Object obj) {
		if(obj instanceof Locations) {
			return Locations.isEquals((Locations) obj, this);
		}
		return false;
	}
}