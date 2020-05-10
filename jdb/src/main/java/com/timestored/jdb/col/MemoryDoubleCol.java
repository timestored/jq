package com.timestored.jdb.col;

import java.util.Arrays;
import java.util.function.Function;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Preconditions;
import com.timestored.jdb.codegen.Genie2;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.function.DoublePredicate;
import com.timestored.jdb.function.MonadToDoubleFunction;
import com.timestored.jdb.function.ToDoubleFunction;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;

public class MemoryDoubleCol extends BaseDoubleCol {

	/** next empty pos for adding items **/
	private int size = 0;
	
	private double[] v;
	
	public MemoryDoubleCol() { this(MemoryCol.DEFAULT_SIZE); }
	
	public MemoryDoubleCol(int initialSize) {
		Preconditions.checkArgument(initialSize >= 0);
		v = new double[initialSize];
		this.setSize(initialSize);
	}

	public MemoryDoubleCol(double... vals) {
		this.v = vals;
		this.setSize(vals.length);
	}

	@Override public void setSize(int newSize) {
		setSpace(newSize);
		size = newSize;
	}
	
	/**
	 * Set the amount of space the backing store allocates.
	 * If the newSize is less than the number of items contained, those items are lost.
	 */
	public void setSpace(int newSize) {
		Preconditions.checkArgument(newSize >= 0);
		if(newSize > v.length) {
			int nSize = v.length == 0 ? MemoryCol.DEFAULT_SIZE : v.length;
			while(nSize < newSize) {
				nSize *=2;
			}
			v = Arrays.copyOf(v, nSize);
		} else if(size > newSize){
			// do not change the backing array, only make it appear shrunk
			size = newSize;
		}
	}
	
	
	@Override public boolean uncheckedAddAll(DoubleIter doubleIterator) {
		setSpace(size + doubleIterator.size());
		while(doubleIterator.hasNext()) {
			v[size++] = doubleIterator.nextDouble();
		}
		return doubleIterator.size() > 0;
	}

	/** This function is provided only for convenience, recommend using {@link #addAll(DoubleCol)} instead **/
	public void add(double val) {
		if(sorted && size > 0) {
			sorted = val >= v[size -1];
		}
		setSpace(size + 1);
		v[size++] = val; 
	}
	

	@Override public double get(int i) { return v[i]; }
	@Override public void uncheckedSet(int idx, double val) { v[idx] = val; }
	@Override public int size() { return size; }
	@Override public void close(){}
	@Override public boolean isAppendable() { return true; }
	@Override public boolean isUpdateable() { return true; }


	@Override public void map(Locations locations, RMode rmode) { 
		if(locations.getMax() > size) {
			setSize(locations.getMax() + 1);
		}
	}

    public static String Camel(String n) {
        return n.substring(0,1) + n.substring(1);
    }
    
	@Override public String toString() {
		StringBuilder sb = new StringBuilder(10+size*2);
		sb.append("Memory" + Camel(CType.getType(type).getQName()) + "Col").append(isSorted() ? "#sorted" : "").append("[");
		if(size > 0) {
			sb.append(v[0]);
			for(int i=1; i<size; i++) {
				sb.append(",").append(v[i]);
			}
		}
		return  sb.append("]").toString();
	}

	@Override public DoubleCol sort() {
		Arrays.parallelSort(v);
		this.sorted = true;
		return this;
	}
}
