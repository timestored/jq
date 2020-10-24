package com.timestored.jdb.kexception;

import java.io.IOException;

public class OsException extends KException {
	private static final long serialVersionUID = 1L;
	public OsException(String description) { super(description); }
	public OsException() { super(); }
	public OsException(IOException e) { super(e.toString()); }
	@Override public String getTitle() { return "os"; }
}
