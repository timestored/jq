package com.timestored.jdb.col;

import com.google.common.base.Preconditions;
import com.timestored.jdb.kexception.LengthException;

public class MyTbl extends MyMapp implements Tbl {

	public MyTbl(StringCol key, ObjectCol value) {
		super(key, value);
		Preconditions.checkArgument(key.size() > 0);
		int rows = ((Col)value.get(0)).size();
		for(int c=0; c<value.size(); c++) {
			if(!(((Col)value.get(c)).size() == rows)) {
				throw new LengthException("Must create table from square dictionaries");
			}
		}
	}

	public MyTbl(Mapp m) { this((StringCol) m.getKey(), (ObjectCol) m.getValue()); }

	@Override public ObjectCol getValue() {
		return (ObjectCol) super.getValue();
	}

	@Override public StringCol getKey() {
		return (StringCol) super.getKey();
	}
	
	@Override public int size() {
		return ((Col) getValue().get(0)).size();
	}
}
