package com.timestored.jdb.database;

import com.timestored.jdb.kexception.KException;

public class LimitException extends KException {
	private static final long serialVersionUID = 1L;
	public String getTitle() { return "limit"; }
	public LimitException() { super(); }
	public LimitException(String description) {super(description); }
}

