package com.timestored.jdb.kexception;

public class LengthException extends KException {
	private static final long serialVersionUID = 1L;
	public LengthException() { super(); }
	public LengthException(String description) { super(description); }
	@Override public String getTitle() { return "length"; }
}
