package com.timestored.jdb.iterator;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

public interface SmartIterator<T> extends Iterator<T>,AutoCloseable,Iterable<T> {

	/**
	 * Reset the iterator to the start of the collection.
	 */
	public void reset();
	
	/**
	 * Moves to the selected position and returns that element.
	 */
	public T get(int idx);
	
	public int size();
	
	public void close() throws IOException;
	
	@Override public SmartIterator<T> iterator();

	public default List<T> toList() { return new SmartList<T>(this); }
	
	static class SmartList<T> extends AbstractList<T> {

		private final SmartIterator<T> si;
		public SmartList(SmartIterator<T> si) { this.si = si; }
		
		@Override public T get(int index) { return si.get(index); }

		@Override public int size() { return si.size(); }
		
	}

	public static <T> SmartIterator<T> fromList(List<T> people) {
		return new ListSmartIterator<T>(people);
	}
	
}
