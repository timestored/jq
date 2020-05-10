package com.timestored.jdb.col;

import com.timestored.jdb.database.CType;
import com.timestored.jdb.kexception.NYIException;

/**
 * Uses a 2-way map to convert to/from one type to another, to allow
 * presenting one type of object while storing it as another.
 * This class is intended to allow generation from a template.
 */
public class BiDelegateLongCol<T> extends BiDelegateCol {

	private final LongCol col;
	private final ToFromLong<T> convert;
	
	public BiDelegateLongCol(CType ctype, LongCol col, ToFromLong<T> convert) {
		super(ctype, col);
		this.col = col;
		this.convert = convert;
	}

	public T max() { return convert.apply(col.max()); }
	public T min() { return convert.apply(col.min()); }
	public T first() { return convert.apply(col.first()); }
	public T last() { return convert.apply(col.last()); }
	public int bin(T val) { return col.bin(convert.applyAsLong(val)); }
	public int binr(T val) { return col.binr(convert.applyAsLong(val)); }
	public void set(int index, T value) {	col.set(index, convert.applyAsLong(value)); }
	public T get(int index) { return convert.apply(col.get(index)); }

	public boolean contains(T needle) { return col.contains(convert.applyAsLong(needle)); }
	public int find(T needle) { return col.find(convert.applyAsLong(needle)); }
	@Override public void setObject(int i, Object value) { col.set(i, convert.applyAsLong((T) value)); }
	@Override public Col sort() { throw new NYIException(); }
}
