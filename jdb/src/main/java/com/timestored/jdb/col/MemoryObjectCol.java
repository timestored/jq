package com.timestored.jdb.col;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.Consts;
import com.timestored.jdb.function.DiadToBooleanFunction;
import com.timestored.jdb.function.DiadToDoubleFunction;
import com.timestored.jdb.function.DiadToObjectFunction;
import com.timestored.jdb.function.MonadToObjectFunction;
import com.timestored.jdb.function.ObjectPairPredicate;
import com.timestored.jdb.iterator.ColDoubleIter;
import com.timestored.jdb.iterator.ColObjectIter;
import com.timestored.jdb.iterator.DoubleIter;
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
			if(contents[i] == null) {
				throw new IllegalStateException("ObjectCol can't contain nulls");
			}
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
	 @Override public ObjectIter select() { 
			// TODO optimize this code, as we can be much smarter
			// no need to create locations, that causes lookup to more locations
			Locations locations = Locations.upTo(this.size());
			return new ColObjectIter(this, locations);
	}
	 

	@Override public ObjectCol select(Locations locations) { 
		MemoryObjectCol dc = new MemoryObjectCol(locations.size());
		for(int i=0; locations.hasNext(); i++) {
			dc.set(i, get(locations.nextInteger()));
		}
		return dc;
	}
	 

	public void add(Object val) {
		sorted = false;
		setSpace(size + 1);
		v[size++] = val; 
	}
	
	 @Override public boolean addAll(ObjectCol ColCol) throws IOException { 
		// TODO optimize this code, as we can be much smarter
		return addAll(ColCol.select());
	}
	@Override public boolean addAll(ObjectIter doubleIterator) throws IOException {
		return uncheckedAddAll(doubleIterator);
	}

	public boolean uncheckedAddAll(ObjectIter objectIter) {
		setSpace(size + objectIter.size());
		while(objectIter.hasNext()) {
			v[size++] = objectIter.nextObject();
		}
		return objectIter.size() > 0;
	}
	 
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
	 @Override public int bin(Object val) { throw new UnsupportedOperationException(); }
	 @Override public int binr(Object val) { throw new UnsupportedOperationException(); }


	/**
	 *  @return The lowest index at which the needle can be found. If there is no match the result is the count. 
	 */
	@Override public int find(Object needle) {
		return scanFind(needle);
	}

	@Override public IntegerCol find(ObjectCol needle) {
		MemoryIntegerCol ic = new MemoryIntegerCol(needle.size());
		ObjectIter it = needle.select();
		int ni = 0;
		while(it.hasNext()) {
			ic.set(ni++, find(it.nextObject()));
		}
		return ic;
	}
	
	int scanFind(Object needle) {
		ObjectIter it = select();
    	int i = 0;
    	while(it.hasNext()) {
    		if(needle.equals(it.nextObject())) {
    			return i;
    		}
    		i++;
    	}
		return i;
	}
	
	@Override
	public Locations select(Locations locations, com.timestored.jdb.function.ObjectPredicate ObjectPredicate) {
		throw new UnsupportedOperationException(); }

	@Override public ObjectCol sort() { throw new UnsupportedOperationException(); }
	@Override public IntegerCol iasc() { throw new UnsupportedOperationException(); }

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
	
	@Override public ObjectCol each(MonadToObjectFunction f) {
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

	@Override public Object over(DiadToObjectFunction f) {
		if(this.size() == 0) {
			return this;
		}
		return over(get(0), f);
	}
	
	@Override public Object over(Object initVal, DiadToObjectFunction f) {
		Object r = initVal;
		for(int i=0; i<size(); i++) {
			r = f.map(r, get(i));
		}
		return r;
	}

	@Override public ObjectCol scan(DiadToObjectFunction f) {
		if(this.size() == 0) {
			return (ObjectCol) ColProvider.emptyCol(getType());
		}
		return scan(get(0), f);
	}
	
	@Override public ObjectCol scan(Object initVal, DiadToObjectFunction f) {
		if(this.size() == 0) {
			return (ObjectCol) ColProvider.emptyCol(getType());
		}
		Object r = initVal;
		MemoryObjectCol dc = new MemoryObjectCol(this.size());
		dc.set(0, r);
		for(int i=1; i<size(); i++) {
			r = f.map(r, get(i));
			dc.set(i, r);
		}
		return dc;
	}


	@Override public ObjectCol eachPrior(DiadToObjectFunction f) {
		if(this.size() == 0) {
			return (ObjectCol) ColProvider.emptyCol(getType());
		}
		return eachPrior(get(0), f);
	}
	
	@Override public ObjectCol eachPrior(Object initVal, DiadToObjectFunction f) {
		if(this.size() == 0) {
			return (ObjectCol) ColProvider.emptyCol(getType());
		}
		MemoryObjectCol dc = new MemoryObjectCol(this.size());
		dc.set(0, initVal);
		for(int i=1; i<size(); i++) {
			Object r = f.map(get(i-1), get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}
	
	@Override public BooleanCol eachPrior(boolean initVal, ObjectPairPredicate f) {
		if(this.size() == 0) {
			return (BooleanCol) ColProvider.emptyCol(getType());
		}
		MemoryBooleanCol dc = new MemoryBooleanCol(this.size());
		dc.set(0, initVal);
		for(int i=1; i<size(); i++) {
			boolean r = f.test(get(i-1), get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}
}
