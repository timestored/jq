package com.timestored.jq;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TableFrame implements Frame {

	private final Tbl tbl;
    private final Frame outerFrame;
	
	@Override public Object get(String id) {
		Col c = tbl.getCol(id);
		return c == null ? outerFrame.get(id) : c; 
	}

	@Override public void assign(String id, Object value) {
		throw new TypeException();
	}

	@Override public Mapp getMapp() { return tbl; }

	@Override public StringCol getKeys() { return tbl.getKey(); }

	@Override public Mapp getMapp(String ns) {
		return outerFrame.getMapp(ns);
	}

}
