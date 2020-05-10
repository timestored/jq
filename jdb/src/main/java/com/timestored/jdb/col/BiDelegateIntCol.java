package com.timestored.jdb.col;

import com.timestored.jdb.database.CType;

public class BiDelegateIntCol<T> extends BiDelegateCol {

	private final IntegerCol col;
	private final ToFromInt<T> convert;
	
	public BiDelegateIntCol(CType ctype, IntegerCol intCol, ToFromInt<T> convert) {
		super(ctype, intCol);
		this.col = intCol;
		this.convert = convert;
	}

	public T max() { return convert.apply(col.max()); }
	public T min() { return convert.apply(col.min()); }
	public T first() { return convert.apply(col.first()); }
	public T last() { return convert.apply(col.last()); }
	public int bin(T val) { return col.bin(convert.applyAsInt(val)); }
	public int binr(T val) { return col.binr(convert.applyAsInt(val)); }
	public void set(int index, T value) {	col.set(index, convert.applyAsInt(value)); }
	public T get(int index) { return convert.apply(col.get(index)); }

	public boolean contains(T needle) { return col.contains(convert.applyAsInt(needle)); }
	public int find(T needle) { return col.find(convert.applyAsInt(needle)); }

	
	@Override public void setObject(int i, Object value) { col.set(i, convert.applyAsInt((T) value)); }

	@Override public Col sort() { col.sort(); return this; }
}
