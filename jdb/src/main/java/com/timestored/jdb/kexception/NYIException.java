package com.timestored.jdb.kexception;

public class NYIException extends KException {
	private static final long serialVersionUID = 1L;
	public NYIException() { super(); }
	public NYIException(String description) { super(description); }
	@Override public String getTitle() { return "nyi"; }
}

