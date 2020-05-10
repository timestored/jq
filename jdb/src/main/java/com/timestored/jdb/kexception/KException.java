package com.timestored.jdb.kexception;

public abstract class KException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public abstract String getTitle();
	public KException(String description) { super(description); }
	public KException(String description, Throwable throwable) { super(description, throwable); }
	public KException() { super(); }
}

