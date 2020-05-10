package com.timestored.jdb.iterator;

import java.io.IOException;
import java.util.List;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ListSmartIterator<T> implements SmartIterator<T> {

	private final List<T> l;
	private int i = 0;

	@Override public boolean hasNext() { return i<l.size(); }
	@Override public T next() { return l.get(i++);}
	@Override public void reset() { i = 0; }
	@Override public T get(int idx) { return l.get(idx); }
	@Override public int size() { return l.size(); }
	@Override public void close() throws IOException { }
	@Override public SmartIterator<T> iterator() { reset(); return this; }

}
