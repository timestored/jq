package com.timestored.jq;

import com.timestored.jdb.kexception.KException;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class IdNotFoundException extends KException {
	private static final long serialVersionUID = 1L;
	@Getter private final String title;
	
	@Override public String toString() { return  title; }
}
