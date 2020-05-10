package com.timestored.jdb.iterator;

public interface IteratorCreator<T> {

	SmartIterator<T> create(Locations locations);
}
