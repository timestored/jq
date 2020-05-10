package com.timestored.jdb.database;

import com.timestored.jdb.kexception.KException;

public class DomainException extends KException {
	private static final long serialVersionUID = 1L;
	public String getTitle() { return "domain"; }
	public DomainException() { super(); }
	public DomainException(String description) {super(description); }
	public DomainException(Exception e) { super(e.toString()); }
}

