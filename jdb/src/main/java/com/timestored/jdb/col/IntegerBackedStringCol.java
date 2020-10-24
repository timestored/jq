package com.timestored.jdb.col;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.function.DiadToDoubleFunction;
import com.timestored.jdb.function.DiadToStringFunction;
import com.timestored.jdb.function.IntegerPredicate;
import com.timestored.jdb.function.MonadToStringFunction;
import com.timestored.jdb.function.StringPairPredicate;
import com.timestored.jdb.function.StringPredicate;
import com.timestored.jdb.iterator.IntegerIter;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.iterator.ObjectIter;
import com.timestored.jdb.iterator.StringIter;
import com.timestored.jdb.predicate.IntegerPredicates;
import com.timestored.jdb.predicate.PredicateFactory;
import com.timestored.jdb.predicate.StringPredicates;
import com.timestored.jdb.predicate.StringPredicates.EqualsStringPredicate;

import lombok.ToString;

class IntegerBackedStringCol extends BiDelegateIntCol<String>  implements StringCol {

	protected final IntegerCol intCol;
	protected final StringMap stringMap = StringMap.INSTANCE;
	
	IntegerBackedStringCol(IntegerCol intCol) {
		super(CType.STRING, intCol, StringMap.INSTANCE);
		this.intCol = Preconditions.checkNotNull(intCol);
	}
	
	@Override public Locations select(Locations locations, StringPredicate predicate) {
		if(predicate instanceof StringPredicates.EqualsStringPredicate) {
			EqualsStringPredicate eq = (EqualsStringPredicate) predicate;
			IntegerPredicate integerPredicate = IntegerPredicates.equal(stringMap.applyAsInt(eq.getV()));
			return intCol.select(locations, integerPredicate);
		}
		
		return intCol.select(locations, (int v) -> predicate.test(stringMap.apply(v)));
	}
	
	
	@Override public boolean addAll(StringIter myStringIterator) throws IOException {
		IntegerIter ii = new IntegerIter() {
			@Override public int size() { return myStringIterator.size(); }
			@Override public void reset() { myStringIterator.reset(); }
			@Override public boolean hasNext() { return myStringIterator.hasNext(); }
			@Override public int nextInteger() { return stringMap.applyAsInt(myStringIterator.nextString()); }
		};
		return intCol.addAll(ii);
	}

	@Override public Locations select(Locations locations, PredicateFactory predicateFactory) {
		return select(locations, predicateFactory.getStringPredicate());
	}

	@Override public StringIter select() {
			IntegerIter ii = intCol.select();
			return new StringIter() {
				@Override public int size() { return ii.size(); }
				@Override public void reset() { ii.reset(); }
				@Override public boolean hasNext() { return ii.hasNext(); }
				@Override public String nextString() { return stringMap.apply(ii.nextInteger()); }
			};
	}

	@Override public StringCol select(Locations locations) {
		return new IntegerBackedStringCol(intCol.select(locations));
	}

	@Override public boolean addAll(StringCol StringCol) throws IOException {
		return intCol.addAll(toIntegerCol(StringCol));
	}

	private IntegerCol toIntegerCol(StringCol stringCol) {
		MemoryIntegerCol m = new MemoryIntegerCol(stringCol.size());
		StringIter it = stringCol.select();
		while(it.hasNext()) {
			m.add(stringMap.applyAsInt(it.nextString()));
		}
		return m;
	}


	@Override public boolean contains(StringCol needle) {
		StringIter it = needle.select();
		while(it.hasNext()) {
			if(!contains(it.nextString())) {
				return false;
			}
		}
		return true;
	}

	/**
	 *  @return The lowest index at which the needle can be found. If there is no match the result is the count. 
	 */
	@Override public int find(String needle) {
		return scanFind(needle);
	}

	@Override public IntegerCol find(StringCol needle) {
		MemoryIntegerCol ic = new MemoryIntegerCol(needle.size());
		StringIter it = needle.select();
		int ni = 0;
		while(it.hasNext()) {
			ic.set(ni++, find(it.nextString()));
		}
		return ic;
	}
	
	int scanFind(String needle) {
		StringIter it = select();
    	int i = 0;
    	while(it.hasNext()) {
    		if(needle.equals(it.nextString())) {
    			return i;
    		}
    		i++;
    	}
		return i;
	}

	@Override public boolean equals(Object obj) {
		if(obj instanceof StringCol) {
			return StringCol.isEquals(this, (StringCol)obj);
		}
		return false;
	}

	@Override public StringCol sort() {
		throw new UnsupportedOperationException();
	}
	@Override public IntegerCol iasc() {
		throw new UnsupportedOperationException();
	}

	@Override public String toString() {
		
		try {
			String[] a = toStringArray();
			Arrays.parallelSort(a);
			return "StringCol[" + Arrays.deepToString(a) + "]";
		} catch (IOException e) {
			return "StringCol[]";
		}
	}
	
	@Override public void setType(short type) { throw new UnsupportedOperationException(); }

	@Override public StringCol map(MonadToStringFunction f) {
		MemoryStringCol dc = new MemoryStringCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i)));
		}
		return dc;
	}
	
	@Override public StringCol map(StringCol b, DiadToStringFunction f) {
		MemoryStringCol dc = new MemoryStringCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i), b.get(i)));
		}
		return dc;
	}

	@Override public String over(DiadToStringFunction f) {
		if(this.size() == 0) {
			return "";
		}
		return over(get(0), f);
	}
	
	@Override public String over(String initVal, DiadToStringFunction f) {
		String r = initVal;
		for(int i=0; i<size(); i++) {
			r = f.map(r, get(i));
		}
		return r;
	}

	@Override public StringCol scan(DiadToStringFunction f) {
		if(this.size() == 0) {
			return (StringCol) ColProvider.emptyCol(getType());
		}
		return scan(get(0), f);
	}
	
	@Override public StringCol scan(String initVal, DiadToStringFunction f) {
		if(this.size() == 0) {
			return (StringCol) ColProvider.emptyCol(getType());
		}
		String r = initVal;
		MemoryStringCol dc = new MemoryStringCol(this.size());
		dc.set(0, r);
		for(int i=1; i<size(); i++) {
			r = f.map(r, get(i));
			dc.set(i, r);
		}
		return dc;
	}

	@Override public StringCol eachPrior(DiadToStringFunction f) {
		if(this.size() == 0) {
			return (StringCol) ColProvider.emptyCol(getType());
		}
		return eachPrior(get(0), f);
	}
	
	@Override public StringCol eachPrior(String initVal, DiadToStringFunction f) {
		if(this.size() == 0) {
			return (StringCol) ColProvider.emptyCol(getType());
		}
		MemoryStringCol dc = new MemoryStringCol(this.size());
		dc.set(0, initVal);
		for(int i=1; i<size(); i++) {
			String r = f.map(get(i-1), get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}
	
	@Override public BooleanCol eachPrior(boolean initVal, StringPairPredicate f) {
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
	
	@Override public StringCol each(MonadToStringFunction f) {
		return ColProvider.s(this.size(), i -> f.map(get(i)));
	}
}
