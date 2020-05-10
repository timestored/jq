package com.timestored.jdb.codegen;

import java.io.Serializable;

import lombok.Data;

@Data
public class Pair<A,B> implements Serializable {
	private static final long serialVersionUID = 1L;
	public final A a;
	public final B b;
}