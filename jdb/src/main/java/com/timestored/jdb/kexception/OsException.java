package com.timestored.jdb.kexception;

public class OsException extends KException {
	private static final long serialVersionUID = 1L;
	public OsException(String description) { super(description); }
	public OsException() { super(); }
	@Override public String getTitle() { return "os"; }
}
