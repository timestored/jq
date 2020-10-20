package com.timestored.jdb.kexception;

import lombok.Data;

@Data
public class NamedException extends KException {
	private static final long serialVersionUID = 1L;
	private final String title;
}
