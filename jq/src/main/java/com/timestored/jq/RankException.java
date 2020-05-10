package com.timestored.jq;

import com.timestored.jdb.kexception.KException;

public class RankException extends KException {
	private static final long serialVersionUID = 1L;
	public RankException() {}
	public RankException(String description) {super(description); }
	@Override public String getTitle() { return "rank"; }
}
