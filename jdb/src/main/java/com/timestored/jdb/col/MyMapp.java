package com.timestored.jdb.col;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jdb.predicate.PredicateFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class MyMapp implements Mapp {

	@Getter private final Col key;
	@Getter private final Col value;
	
	public MyMapp(Col key, Col value) {
		if(key.size() != value.size()) {
			throw new LengthException("Key and value of dictionary must be the same length. Key length: " + key.size() + " value length: " + value.size());
		}
		this.key = Preconditions.checkNotNull(key);
		this.value = Preconditions.checkNotNull(value);
	}
	
	@Override public Locations select(Locations locations, PredicateFactory predicateFactory) {
		return getValue().select(locations, predicateFactory);
	}
	
	@Override public Col select(Locations locations) {
		return getValue().select(locations);
	}

	@Override public int size() {
		return getKey().size();
	}

	@Override public boolean isAppendable() {
		return getKey().isAppendable() && getValue().isAppendable();
	}

	@Override public boolean isUpdateable() {
		return getKey().isUpdateable() && getValue().isUpdateable();
	}

	@Override public boolean isSorted() { return key.isSorted(); }

	@Override public boolean applySorted() { return key.applySorted(); }

	@Override public void map(Locations locations, RMode rmode) throws IOException {
		getKey().map(locations, rmode);
		getValue().map(locations, rmode);
	}

	@Override public short getType() { return CType.MAPP.getTypeNum(); }

	@Override public short getSizeInBytes() {
		return (short) (getKey().getSizeInBytes() + getValue().getSizeInBytes());
	}

	@Override public void setSize(int newSize) throws IOException {
		getKey().setSize(newSize);
		getValue().setSize(newSize);
	}

	@Override public void close() throws Exception {
		getKey().close();
		getValue().close();
	}

	@Override public void setType(short type) { }
	@Override public void setSorted(boolean sorted) { key.setSorted(sorted); }

	@Override public void setObject(int i, Object object) {
		throw new NYIException();
	}

	@Override public Col sort() { throw new NYIException(); }
	@Override public IntegerCol iasc() { throw new NYIException(); }

}
