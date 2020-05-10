/** This code was generated using code generator:com.timestored.jdb.col**/
package com.timestored.jdb.col;

public class MemoryStringCol extends IntegerBackedStringCol {
	
	public MemoryStringCol(int size) {
		super(new MemoryIntegerCol(size));
	}

	public MemoryStringCol(String val) {
		super(new MemoryIntegerCol(1));
		set(0, val);
	}

	public void add(String k) {
		((MemoryIntegerCol)intCol).add(StringMap.INSTANCE.applyAsInt(k));
	}

}
