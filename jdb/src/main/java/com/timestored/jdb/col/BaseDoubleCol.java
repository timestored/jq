package com.timestored.jdb.col;

import java.io.IOException;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;
import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Preconditions;
import com.timestored.jdb.database.DataReader;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Consts;
import com.timestored.jdb.function.BooleanPairPredicate;
import com.timestored.jdb.function.DiadToBooleanFunction;
import com.timestored.jdb.function.DiadToDoubleFunction;
import com.timestored.jdb.function.DoublePairPredicate;
import com.timestored.jdb.function.DoublePredicate;
import com.timestored.jdb.function.MonadToBooleanFunction;
import com.timestored.jdb.function.MonadToDoubleFunction;
import com.timestored.jdb.iterator.ColDoubleIter;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.SortException;
import com.timestored.jdb.predicate.DoublePredicates;
import com.timestored.jdb.predicate.DoublePredicates.BetweenDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.EqualsDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.GreaterThanDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.GreaterThanOrEqualDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.InDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.LessThanDoublePredicate;
import com.timestored.jdb.predicate.DoublePredicates.LessThanOrEqualDoublePredicate;
import com.timestored.jdb.predicate.ShortPredicates.EqualsShortPredicate;
import com.timestored.jdb.predicate.PredicateFactory;

import lombok.Getter;
import lombok.Setter;

/**
 * Implements some handy functions while leaving how they work to the actual implementing classes. 
 */
abstract class BaseDoubleCol implements DoubleCol {

	@Getter protected boolean sorted = !Database.QCOMPATIBLE;
	@Getter protected short type = (short) (-1 * CType.DOUBLE.getTypeNum());
	
	@Override public boolean addAll(DoubleCol doubleCol) throws IOException {
		// TODO optimize this code, as we can be much smarter
		return addAll(doubleCol.select());
	}

	@Override public boolean addAll(DoubleIter doubleIterator) throws IOException {
		if(sorted && doubleIterator.size() > 0) {
			double prevD = Consts.Mins.DOUBLE;
			boolean newDataSorted = true;
			while(doubleIterator.hasNext()) {
				double d = doubleIterator.nextDouble();
				if(d < prevD) {
					newDataSorted = false;
					break;
				}
				prevD = d;
			}
			doubleIterator.reset();
			double firstVal = doubleIterator.nextDouble();
			doubleIterator.reset();
			sorted = newDataSorted;
			// check that first new value is greater than or equal last known value.
			if(sorted && size() > 0) {
				int lastPos = size()-1;
				// using write as the uncheckedAddAll will also use write, so prevents change of access i.e. faster hopefully
				map(Locations.forRange(lastPos, lastPos+1), RMode.WRITE);
				sorted = firstVal >= get(lastPos);
			}
		}
		return uncheckedAddAll(doubleIterator);
	}
	
	abstract boolean uncheckedAddAll(DoubleIter myDoubleIterator) throws IOException;
	
	

	@Override public Locations select(Locations locations, DoublePredicate predicate) {

		// The location thing is a hack until I fix the setBounds calls to work for the offsets
		if(isSorted() && locations.getMin()==0 && locations.getMax()==size()-1) {
			if(predicate instanceof DoublePredicates.GreaterThanDoublePredicate) {
				GreaterThanDoublePredicate gt = (DoublePredicates.GreaterThanDoublePredicate) predicate;
				int p = bin(gt.getLowerBound());
				return p == size() ? Locations.EMPTY : locations.setBounds(p + 1, size());
			} else if(predicate instanceof DoublePredicates.LessThanOrEqualDoublePredicate) {
				LessThanOrEqualDoublePredicate lte = (DoublePredicates.LessThanOrEqualDoublePredicate) predicate;
				int p = bin(lte.getUpperBound());
				return locations.setBounds(0, p + 1);
			} else if(predicate instanceof DoublePredicates.LessThanDoublePredicate) {
				LessThanDoublePredicate lt = (DoublePredicates.LessThanDoublePredicate) predicate;
				int p = binr(lt.getUpperBound());
				return locations.setBounds(0, p);
			} else if(predicate instanceof DoublePredicates.GreaterThanOrEqualDoublePredicate) {
				GreaterThanOrEqualDoublePredicate gte = (DoublePredicates.GreaterThanOrEqualDoublePredicate) predicate;
				int p = binr(gte.getLowerBound());
				return locations.setBounds(p, size());
			} else if(predicate instanceof DoublePredicates.BetweenDoublePredicate) {
				BetweenDoublePredicate bt = (DoublePredicates.BetweenDoublePredicate) predicate;
				// TODO optimize this further by letting second call take account of first call.
				return findBetween(locations, bt.getLowerBound(), bt.getUpperBound());
			} else if(predicate instanceof DoublePredicates.EqualsDoublePredicate) {
				EqualsDoublePredicate eq = (DoublePredicates.EqualsDoublePredicate) predicate;
				return findBetween(locations, eq.getV(), eq.getV());
			}
		}
		
		try {
			map(locations, RMode.READ);
		} catch (IOException e) {
			throw new IllegalStateException("Could not map the col", e);
		}
		IntArrayList a = new IntArrayList(locations.size());
		while(locations.hasNext()) {
			int pos = locations.nextInteger();
			double val = get(pos);
			if(predicate.test(val)) {
				a.add(pos);
			}
		}
		return Locations.wrap(a);
	}

	private Locations findBetween(Locations locations, double low, double high) {
		int p = binr(low);
		if(p == size()) {
			return Locations.EMPTY;
		}
		int q = bin(high);
		return locations.setBounds(p, q + 1);
	}
	
	
	
	@Override public void set(int index, double value) {
		try {	
			if(sorted) {
				boolean lowerOK = (index == 0) || (value > get(index - 1));
				boolean upperOK = (index == size()-1) || (value < get(index + 1));
				sorted = lowerOK && upperOK;
			}
			uncheckedSet(index, value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	abstract void uncheckedSet(int index, double value) throws IOException;
	
	@Override public DoubleIter select() {
		// TODO optimize this code, as we can be much smarter
		// no need to create locations, that causes lookup to more locations
		Locations locations = Locations.upTo(this.size());
		mapForRead(locations);
		return new ColDoubleIter(this, locations);
	}

	private void mapForRead(Locations locations) {
		try {
			map(locations, RMode.READ);
		} catch (IOException e) {
			throw new IllegalStateException("Could not map the col", e);
		}
	}

	@Override public DoubleCol select(Locations locations) { 
		mapForRead(locations);
		MemoryDoubleCol dc = new MemoryDoubleCol(locations.size());
		for(int i=0; locations.hasNext(); i++) {
			dc.set(i, get(locations.nextInteger()));
		}
		return dc;
	}
	
	@Override public Locations select(Locations locations, PredicateFactory predicateFactory) {
		return select(locations, predicateFactory.getDoublePredicate());
	}


	@Override public boolean equals(Object obj) {
		if(obj instanceof DoubleCol) {
			return DoubleCol.isEquals((DoubleCol) obj, this);
		}
		return false;
	}
	

	@Override public boolean contains(DoubleCol needle) {
		DoubleIter it = needle.select();
		while(it.hasNext()) {
			if(find(it.nextDouble()) == size()) {
				return false;
			}
		}
		return true;
	}
	

	/** {@inheritDoc}  */
	@Override public int binr(double val) {
		if(size() == 0) {
			return -1;
		}
		int low = 0;
		int high = size() - 1;
		mapForRead(Locations.forRange(low, high));
		int mid = (low+high) >>> 1;
		
		while(low <= high) {
			double midVal = get(mid);
			if (val > midVal){
				low = mid +1;
			} else if (val < midVal){
				high = mid -1;
			} else if(low != mid) {
				high = mid - 1;
			} else {
				return mid; 
			}
			mid = (low+high) >>> 1;
		}
		return low;
	}
	
	
	/** {@inheritDoc}  */
	@Override public int bin(double val) {
		if(size() == 0) {
			return -1;
		}
		int low = 0;
		int high = size() - 1;
		mapForRead(Locations.forRange(low, high));
		int mid = (low+high) >>> 1;
		
		while(low <= high) {
			double midVal = get(mid);
			if (val > midVal){
				low = mid +1;
			} else if (val < midVal){
				high = mid -1;
			} else if(high != mid) {
				low = mid + 1;
			} else {
				return mid; 
			}
			mid = (low+high) >>> 1;
		}
		return low == 0 ? low - 1 : (high == size()-1 ? high+1 : high);
	}
	
	/**
	 *  @return The lowest index at which the needle can be found. If there is no match the result is the count. 
	 */
	@Override public int find(double needle) {
		if(size() > 4 && isSorted()) {
			return binaryFind(needle);
		} else {
			return scanFind(needle);
		}
	}

	int binaryFind(double needle) {
		int low = 0;
		int high = size() - 1;
		mapForRead(Locations.forRange(low, high));

		while(low <= high) {
			int mid = (low+high) >>> 1;
			double midVal = get(mid);
			if (needle > midVal){
				low = mid +1;
			} else if (needle < midVal){
				high = mid -1;
			} else if(low != mid) {
				high = mid;
			} else {
				return mid; 
			}
		}
		return size();
	}

	
	int scanFind(double needle) {
		DoubleIter it = select();
    	int i = 0;
    	while(it.hasNext()) {
    		if(needle == it.nextDouble()) {
    			return i;
    		}
    		i++;
    	}
		return i;
	}

	@Override public IntegerCol find(DoubleCol needle) {
		MemoryIntegerCol ic = new MemoryIntegerCol(needle.size());
		DoubleIter it = needle.select();
		int ni = 0;
		while(it.hasNext()) {
			ic.set(ni++, find(it.nextDouble()));
		}
		return ic;
	}
	
	@Override public boolean contains(double needle) {
		return find(needle) != size();
	}
	
	@Override public double max() {
		double max = Consts.Mins.DOUBLE;
		if(size() > 0 && isSorted()) {
			max = last();
		} else {
			DoubleIter it = select();
			while(it.hasNext()) {
				double n = it.nextDouble();
				if(n > max && !SpecialValues.isNull(n)) {
					max = n;
				}
			}
		}
		return max;
	}
	
	@Override public double min() {
		double min = Consts.Maxs.DOUBLE;

		if(size() > 0 && isSorted()) {
			min = first();
		} else {
			DoubleIter it = select();
			while(it.hasNext()) {
				double n = it.nextDouble();
				if(n < min && !SpecialValues.isNull(n)) {
					min = n;
				}
			}
		}
		return min;
	}
	
	@Override public double first() {
		if(size() > 0) {
			mapForRead(Locations.of(0));
			return get(0);
		}
		return Consts.Nulls.DOUBLE;
	}
	
	@Override public double last() {
		int sz = size();
		if(sz > 0) {
			mapForRead(Locations.of(sz - 1));
			return get(sz - 1);
		}
		return Consts.Nulls.DOUBLE;
	}
	
	@Override public boolean applySorted() {
		if(!sorted) {
			double prevVal = Consts.Mins.DOUBLE;
			boolean allSorted = true;
			DoubleIter it = select();
			while(it.hasNext()) {
				double v = it.nextDouble();
				if(v < prevVal) {
					allSorted = false;
					break;
				}
				prevVal = v;
			}
			sorted = allSorted;
		}
		return sorted;
	}
	
	@Override public void setSorted(boolean sorted) {
		if(this.sorted) {
			this.sorted = false; // Foolish removing this flag but required for compatibility
		} else if(sorted) {
			if(!applySorted()) {
				throw new SortException();
			}
		}
	}
	
	@Override public void setObject(int i, Object object) {
		this.set(i, (double) object);
	}
	
	@Override public DoubleCol map(MonadToDoubleFunction f) {
		return new ProjectedDoubleCol(size(), getType(), i -> f.map(get(i)));
	}
	
	@Override public DoubleCol map(DoubleCol b, DiadToDoubleFunction f) {
    	if(size() != b.size()) {
    		throw new LengthException();
    	}
		return new ProjectedDoubleCol(size(), getType(), i -> f.map(get(i), b.get(i)));
	}

	@Override public double over(DiadToDoubleFunction f) {
		if(this.size() == 0) {
			return 0;
		}
		return over(get(0), f);
	}
	
	@Override public double over(double initVal, DiadToDoubleFunction f) {
		double r = initVal;
		for(int i=0; i<size(); i++) {
			r = f.map(r, get(i));
		}
		return r;
	}
	

	@Override public DoubleCol scan(DiadToDoubleFunction f) {
		if(this.size() == 0) {
			return (DoubleCol) ColProvider.emptyCol(getType());
		}
		return scan(get(0), f);
	}
	
	@Override public DoubleCol scan(double initVal, DiadToDoubleFunction f) {
		if(this.size() == 0) {
			return (DoubleCol) ColProvider.emptyCol(getType());
		}
		double r = initVal;
		MemoryDoubleCol dc = new MemoryDoubleCol(this.size());
		for(int i=0; i<size(); i++) {
			r = f.map(r, get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}

	@Override public DoubleCol eachPrior(DiadToDoubleFunction f) {
		if(this.size() == 0) {
			return (DoubleCol) ColProvider.emptyCol(getType());
		}
		return eachPrior(get(0), f);
	}
	
	@Override public DoubleCol eachPrior(double initVal, DiadToDoubleFunction f) {
		if(this.size() == 0) {
			return (DoubleCol) ColProvider.emptyCol(getType());
		}
		MemoryDoubleCol dc = new MemoryDoubleCol(this.size());
		dc.set(0, initVal);
		for(int i=1; i<size(); i++) {
			double r = f.map(get(i-1), get(i));
			dc.set(i, r);
		}
		dc.setType(getType());
		return dc;
	}
	
	@Override public BooleanCol eachPrior(boolean initVal, DoublePairPredicate f) {
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
	
	@Override public DoubleCol each(MonadToDoubleFunction f) {
		return new ProjectedDoubleCol(this.size(), this.type, i -> f.map(get(i)));
	}
	
	@Override public void setType(short type) {
		Preconditions.checkArgument(type >= 0);
		this.type = type;
	}
}
