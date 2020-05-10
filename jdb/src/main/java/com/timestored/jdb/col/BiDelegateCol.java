package com.timestored.jdb.col;

import java.io.IOException;

import com.timestored.jdb.database.CType;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.predicate.PredicateFactory;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * A Col that maps all Col calls from one instance to another
 */
@RequiredArgsConstructor
public abstract class BiDelegateCol implements Col {

	private final CType ctype;
	private final Col col;

	@Override public Locations select(Locations locations, PredicateFactory predicateFactory) { return col.select(locations, predicateFactory); }
	@Override public Col select(Locations locations) { return col.select(locations); }
	@Override public int size() { return col.size(); }
	@Override public boolean isAppendable() { return col.isAppendable(); }
	@Override public boolean isUpdateable() { return col.isUpdateable(); }
	@Override public boolean isSorted() { return col.isSorted(); }
	@Override public boolean applySorted() { return col.applySorted(); }
	@Override public void map(Locations locations, RMode rmode) throws IOException { col.map(locations, rmode); }
	@Override public short getSizeInBytes() { return col.getSizeInBytes(); }
	@Override public void setSize(int newSize) throws IOException { col.setSize(newSize);  }
	@Override public void close() throws Exception { col.close(); }
	@Override public short getType() {  return ctype.getTypeNum(); }
	@Override public void setType(short type) { }
	
	@Override public void setSorted(boolean sorted) {
		if(sorted) {
			throw new UnsupportedOperationException();
		}
	}
}
