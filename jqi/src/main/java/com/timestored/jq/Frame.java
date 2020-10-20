package com.timestored.jq;

import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;

public interface Frame {

	Object get(String id);

	void assign(String id, Object value);

	Mapp getMapp();

	StringCol getKeys();

	public default StringCol getVariables(String ns) {
		Mapp m = getMapp(ns);
		if(m.getKey() instanceof StringCol) {
			return (StringCol) m.getKey();
		}
		throw new TypeException("Key wasn't stings for ns:" + ns);
	}

	Mapp getMapp(String ns);
}