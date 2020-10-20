package com.timestored.jq;

import com.timestored.jdb.kexception.KException;

public class TypeException extends KException {
	private static final long serialVersionUID = 1L;
	public TypeException(String description) { super(description); }
	public TypeException() { super(); }
	public TypeException(ClassCastException e) { super(e.toString()); }
	@Override public String getTitle() { return "type"; }
}
