package com.timestored.jdb.col;

import java.io.RandomAccessFile;

public enum RMode {
	READ("r"),WRITE("rw");
	
	/** Set the {@link RandomAccessFile} flag  **/
	private RMode(String RAFFlag) { this.RAFFlag = RAFFlag; }
	
	private final String RAFFlag;
	
	public String getRandomAccessFileFlag() {
		return RAFFlag;
	}
}