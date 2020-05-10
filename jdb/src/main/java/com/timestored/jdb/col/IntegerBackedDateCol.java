package com.timestored.jdb.col;

import java.io.IOException;
import java.sql.Date;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.function.DatePredicate;
import com.timestored.jdb.function.DiadToDateFunction;
import com.timestored.jdb.function.MonadToDateFunction;
import com.timestored.jdb.iterator.DateIter;
import com.timestored.jdb.iterator.IntegerIter;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.predicate.PredicateFactory;

import lombok.Getter;

public class IntegerBackedDateCol extends BiDelegateIntCol<Date> implements DateCol {
	
	@Getter protected final IntegerCol c;
	protected ToFromInt<Date> m;
	
	public IntegerBackedDateCol(IntegerCol col, ToFromInt<Date> mapper) {
		super(CType.DATE, col, mapper);
		this.c = Preconditions.checkNotNull(col);
		this.m = Preconditions.checkNotNull(mapper);
	}
	
	private IntegerCol toInteger(DateCol colA) {
		if(colA instanceof IntegerBackedDateCol) {
			IntegerBackedDateCol d = ((IntegerBackedDateCol) colA);
			return d.c;
		}
		
		MemoryIntegerCol l = new MemoryIntegerCol(colA.size());
		DateIter it = colA.select();
		while(it.hasNext()) {
			l.add(m.applyAsInt(it.nextDate()));
		}
		return l;
	}

	private IntegerIter toInteger(DateIter itA) {
		MemoryIntegerCol l = new MemoryIntegerCol(itA.size());
		while(itA.hasNext()) {
			l.add(m.applyAsInt(itA.nextDate()));
		}
		itA.reset();
		return l.select();
	}
	
	@Override public Locations select(Locations locations, PredicateFactory predicateFactory) {
		return c.select(locations, predicateFactory.getIntegerPredicate());
	}
	
	@Override public DateIter select() {
		IntegerIter i = c.select();
		return new DateIter() {
			@Override public int size() { return i.size(); }
			@Override public void reset() { i.reset(); }
			@Override public Date nextDate() { return m.apply(i.nextInteger()); }
			@Override public boolean hasNext() { return i.hasNext(); }
		};
	}
	
	@Override public DateCol select(Locations locations) {
		return new IntegerBackedDateCol(c.select(locations), m);
	}
	
	@Override public Locations select(Locations locations, DatePredicate p) {
		// TODO this is a very naive way to do it!
		return c.select(locations, (int v) -> p.test(m.apply(v)));
	}
	
	@Override public boolean addAll(DateIter newDataIter) throws IOException {
		return c.addAll(toInteger(newDataIter));
	}
	
	@Override public boolean addAll(DateCol newDataCol) throws IOException {
		return c.addAll(toInteger(newDataCol));
	}

	public void set(int i, int val) { c.set(i, val);}

	@Override public boolean contains(DateCol needle) {return c.contains(toInteger(needle));}
	@Override public IntegerCol find(DateCol needle) {return c.find(toInteger(needle));}

	@Override public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}
		if(obj instanceof IntegerBackedDateCol) {
			return ((IntegerBackedDateCol)obj).c.equals(this.c);
		}
		if(obj instanceof DateCol) {
			return DateIter.isEquals(((DateCol)obj).select(), this.select()); 
		}
		return false;
	}

	@Override public DateCol sort() {
		c.sort();
		return this;
	}

	@Override public void setType(short type) { throw new UnsupportedOperationException(); }

	@Override public DateCol map(MonadToDateFunction f) {
		MemoryDateCol dc = new MemoryDateCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i)));
		}
		return dc;
	}

	@Override public DateCol map(DateCol b, DiadToDateFunction f) {
		MemoryDateCol dc = new MemoryDateCol(this.size());
		for(int i=0; i<size(); i++) {
			dc.set(i, f.map(get(i), b.get(i)));
		}
		return dc;
	}
}