package com.timestored.jdb.col;

import java.sql.Date;
import java.sql.Timestamp;

public class MemoryDateCol extends IntegerBackedDateCol {

	public MemoryDateCol(int initialSize) {
		super(new MemoryIntegerCol(initialSize), ToFromIntDate.INSTANCE);
	}

	public void add(Date v) { ((MemoryIntegerCol)c).add(m.applyAsInt(v)); }
}
