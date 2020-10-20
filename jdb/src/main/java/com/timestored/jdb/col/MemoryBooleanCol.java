package com.timestored.jdb.col;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.Consts;
import com.timestored.jdb.function.BooleanPairPredicate;
import com.timestored.jdb.function.BooleanPredicate;
import com.timestored.jdb.function.DiadToBooleanFunction;
import com.timestored.jdb.function.DiadToDoubleFunction;
import com.timestored.jdb.function.MonadToBooleanFunction;
import com.timestored.jdb.iterator.BooleanIter;
import com.timestored.jdb.iterator.ColBooleanIter;
import com.timestored.jdb.iterator.ColDoubleIter;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.kexception.SortException;
import com.timestored.jdb.predicate.PredicateFactory;

/**
 * A copy-paste of MemoryDoubleCol for now.
 */
public class MemoryBooleanCol implements BooleanCol {

	/** next empty pos for adding items **/
	private int size = 0;
	private boolean sorted = false;
	
	private boolean[] v;
	
	public MemoryBooleanCol() { this(MemoryCol.DEFAULT_SIZE); }
	
	public MemoryBooleanCol(int initialSize) {
		Preconditions.checkArgument(initialSize >= 0);
		v = new boolean[initialSize];
		this.size = initialSize;
	}

	public MemoryBooleanCol(boolean... contents) {
		this.v = contents;
		this.size = contents.length;
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
	

	/** This function is provided only for convenience, recommend using {@link #addAll(BooleanCol)} instead **/
	public void add(boolean val) {
		if(sorted && size > 0) {
			sorted = val || !val && !v[size -1] ;
		}
		setSpace(size + 1);
		v[size++] = val; 
	}
	

	@Override public boolean get(int i) { return v[i]; }
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
		StringBuilder sb = new StringBuilder(10+size*2);
		sb.append("MemoryBooleanCol").append(isSorted() ? "#sorted" : "").append("[");
		if(size > 0) {
			sb.append(v[0]);
			for(int i=1; i<size; i++) {
				sb.append(",").append(v[i]);
			}
		}
		return  sb.append("]").toString();
	}

	@Override public BooleanCol sort() {
		int sum = 0;
		for(int i=0; i<v.length; i++) {
			sum += v[i] ? 1 : 0;
		}
		BooleanCol bc = new MemoryBooleanCol(size());
		for(int i=sum; i<bc.size(); i++) {
			bc.set(i, true);
		}
		return bc;	
	}
	
	@Override public IntegerCol iasc() {
		return new MemoryIntegerCol(ArrayUtils.iasc(v));
	}

	@Override
	public Locations select(Locations locations, PredicateFactory predicateFactory) {
		throw new UnsupportedOperationException();	}

	@Override public boolean isSorted() { return sorted; }

	@Override public boolean applySorted() {
		throw new UnsupportedOperationException();	}

	@Override
	public BooleanIter select() {
		Locations locations = Locations.upTo(this.size());
		return new ColBooleanIter(this, locations);	}

	@Override public BooleanCol select(Locations locations) {
		MemoryBooleanCol dc = new MemoryBooleanCol(locations.size());
		for(int i=0; locations.hasNext(); i++) {
			dc.set(i, get(locations.nextInteger()));
		}
		return dc;
	}

	@Override
	public Locations select(Locations locations, BooleanPredicate booleanPredicate) {
		throw new UnsupportedOperationException();	}

	@Override
	public boolean addAll(BooleanIter booleanIterator) throws IOException {
		throw new UnsupportedOperationException();	}

	@Override
	public boolean addAll(BooleanCol booleanCol) throws IOException {
		throw new UnsupportedOperationException();	}

	@Override public void set(int i, boolean val) {
		v[i] = val;
	}

	@Override
	public boolean max() {
		for(int i=0; i<v.length; i++) {
			if(v[i]) {
				return true;
			}
		}
		return false;
	}

	@Override public boolean min() {
		for(int i=0; i<v.length; i++) {
			if(!v[i]) {
				return false;
			}
		}
		return true;
	}

	@Override public boolean first() { return v.length>0 ? v[0] : false; }

	@Override public boolean last() {return v.length>0 ? v[v.length-1] : false; }

	@Override
	public boolean contains(BooleanCol needle) {
		throw new UnsupportedOperationException();	}

	@Override
	public boolean contains(boolean needle) {
		throw new UnsupportedOperationException();	}


	@Override public IntegerCol find(BooleanCol needle) {
		MemoryIntegerCol ic = new MemoryIntegerCol(needle.size());
		BooleanIter it = needle.select();
		int ni = 0;
		while(it.hasNext()) {
			ic.set(ni++, find(it.nextBoolean()));
		}
		return ic;
	}

	@Override public int find(boolean needle) { return scanFind(needle); }

	int scanFind(boolean needle) {
		BooleanIter it = select();
    	int i = 0;
    	while(it.hasNext()) {
    		if(needle == it.nextBoolean()) {
    			return i;
    		}
    		i++;
    	}
		return i;
	}	
	
	@Override
	public int bin(boolean val) {
		throw new UnsupportedOperationException();	}

	@Override
	public int binr(boolean val) {
		throw new UnsupportedOperationException();	}
	

	@Override public void setSorted(boolean sorted) {
		if(this.sorted) {
			if(!sorted) {
				this.sorted = false; // Foolish removing this flag but required for compatibility
			}
		} else if(sorted) {
			if(!applySorted()) {
				throw new SortException();
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + size;
		result = prime * result + (sorted ? 1231 : 1237);
		result = prime * result + Arrays.hashCode(v);
		return result;
	}

	@Override public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MemoryBooleanCol other = (MemoryBooleanCol) obj;
		if (size != other.size)
			return false;
		if (sorted != other.sorted)
			return false;
		if (!Arrays.equals(v, other.v))
			return false;
		return true;
	}

	@Override public void setType(short type) {}

	@Override public void setObject(int i, Object object) { set(i, (boolean) object); }

	@Override public BooleanCol map(MonadToBooleanFunction f) {
		MemoryBooleanCol dc = new MemoryBooleanCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i)));
		}
		return dc;
	}
	
	@Override public BooleanCol map(BooleanCol b, DiadToBooleanFunction f) {
		MemoryBooleanCol dc = new MemoryBooleanCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i), b.get(i)));
		}
		return dc;
	}
	

	@Override public boolean over(DiadToBooleanFunction f) {
		if(this.size() == 0) {
			return false;
		}
		return over(get(0), f);
	}
	
	@Override public boolean over(boolean initVal, DiadToBooleanFunction f) {
		boolean r = initVal;
		for(int i=0; i<size(); i++) {
			r = f.map(r, get(i));
		}
		return r;
	}

	@Override public BooleanCol scan(DiadToBooleanFunction f) {
		if(this.size() == 0) {
			return (BooleanCol) ColProvider.emptyCol(getType());
		}
		return scan(get(0), f);
	}
	
	@Override public BooleanCol scan(boolean initVal, DiadToBooleanFunction f) {
		if(this.size() == 0) {
			return (BooleanCol) ColProvider.emptyCol(getType());
		}
		boolean r = initVal;
		MemoryBooleanCol dc = new MemoryBooleanCol(this.size());
		dc.set(0, r);
		for(int i=1; i<size(); i++) {
			r = f.map(r, get(i));
			dc.set(i, r);
		}
		return dc;
	}
	


	@Override public BooleanCol eachPrior(DiadToBooleanFunction f) {
		if(this.size() == 0) {
			return (BooleanCol) ColProvider.emptyCol(getType());
		}
		return eachPrior(get(0), f);
	}
	
	@Override public BooleanCol eachPrior(boolean initVal, DiadToBooleanFunction f) {
		if(this.size() == 0) {
			return (BooleanCol) ColProvider.emptyCol(getType());
		}
		MemoryBooleanCol dc = new MemoryBooleanCol(this.size());
		dc.set(0, initVal);
		for(int i=1; i<size(); i++) {
			boolean r = f.map(get(i-1), get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}
	
	@Override public BooleanCol eachPrior(boolean initVal, BooleanPairPredicate f) {
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
	
	@Override public BooleanCol each(MonadToBooleanFunction f) {
		return ColProvider.b(this.size, i -> f.map(get(i)));
	}
}
