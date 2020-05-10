package com.timestored.jdb.col;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.timestored.jdb.function.DiadToObjectFunction;
import com.timestored.jdb.function.MonadToObjectFunction;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.iterator.ObjectIter;
import com.timestored.jdb.predicate.PredicateFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/* Copy paste of MemoryDoubleCol. Plus handcrafted code */
@EqualsAndHashCode
public class MemoryObjectCol implements ObjectCol {

	/** next empty pos for adding items **/
	private int size = 0;
	
	private Object[] v;

	@Getter private boolean sorted = false;
	
	public MemoryObjectCol() { this(MemoryCol.DEFAULT_SIZE); }
	
	public MemoryObjectCol(int initialSize) {
		Preconditions.checkArgument(initialSize >= 0);
		v = new Object[initialSize]; 
		this.size = initialSize;
	}

	public static MemoryObjectCol of(Object... contents) {
		MemoryObjectCol moc = new MemoryObjectCol(contents.length);
		for(int i=0; i<contents.length; i++) {
			moc.set(i, contents[i]);
		}
		return moc;
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
	
	


	@Override public Object get(int i) { return v[i]; }
	@Override public int size() { return size; }
	@Override public void close(){}
	@Override public boolean isAppendable() { return true; }
	@Override public boolean isUpdateable() { return true; }


	@Override public void map(Locations locations, RMode rmode) { 
		if(locations.getMax() > size) {
			setSize(locations.getMax() + 1);
		}
	}

	@Override public String toString() {
		return (isSorted() ? "sorted" : "") + Arrays.toString(v);
	}

	@Override
	public Locations select(Locations locations, PredicateFactory predicateFactory) {
		throw new UnsupportedOperationException(); 
	}

	 @Override public boolean applySorted() { this.sorted = true; return true; }
	 @Override public ObjectIter select() { throw new UnsupportedOperationException(); }
	 

	@Override public ObjectCol select(Locations locations) { 
		MemoryObjectCol dc = new MemoryObjectCol(locations.size());
		for(int i=0; locations.hasNext(); i++) {
			dc.set(i, get(locations.nextInteger()));
		}
		return dc;
	}
	 
	 @Override public boolean addAll(ObjectIter ColIterator) throws IOException { return false; }
	 @Override public boolean addAll(ObjectCol ColCol) throws IOException { return false; }
	 @Override public void set(int i, Object val) { v[i] = val; }
	 @Override public Object max() { throw new UnsupportedOperationException(); }
	 @Override public Object min() { throw new UnsupportedOperationException(); }
	 @Override public Object first() { return size>0 ? v[0] : null; }
	 @Override public Object last() { return size>0 ? v[size-1] : null; }
	 @Override public boolean contains(ObjectCol needle) { throw new UnsupportedOperationException(); }
	 @Override public boolean contains(Object needle) {
		 if(needle == null) {
			 return false;
		 }
		 for(int i=0; i<size;i++) {
			 if(needle.equals(v[i])) {
				 return true;
			 }
		 }
		 return false; 
	}
	 @Override public IntegerCol find(ObjectCol needle) { throw new UnsupportedOperationException(); }
	 @Override public int find(Object needle) { throw new UnsupportedOperationException(); }
	 @Override public int bin(Object val) { throw new UnsupportedOperationException(); }
	 @Override public int binr(Object val) { throw new UnsupportedOperationException(); }

	@Override
	public Locations select(Locations locations, com.timestored.jdb.function.ObjectPredicate ObjectPredicate) {
		throw new UnsupportedOperationException(); }

	@Override public ObjectCol sort() { throw new UnsupportedOperationException(); }

	@Override public void setSorted(boolean sorted) { this.sorted = sorted; }
	@Override public void setType(short type) { throw new UnsupportedOperationException(); }

	@Override public void setObject(int i, Object val) { set(i, val); }

	@Override public ObjectCol map(MonadToObjectFunction f) {
		MemoryObjectCol dc = new MemoryObjectCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i)));
		}
		return dc;
	}
	
	@Override public ObjectCol map(ObjectCol b, DiadToObjectFunction f) {
		MemoryObjectCol dc = new MemoryObjectCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i), b.get(i)));
		}
		return dc;
	}

}
