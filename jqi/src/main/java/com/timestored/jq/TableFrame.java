package com.timestored.jq;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jq.ops.XkeyOp;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TableFrame implements Frame {

	private final Tbl tbl;
    private final Frame outerFrame;
	
    public TableFrame(Mapp m, Frame outerFrame) {
    	this.outerFrame = Preconditions.checkNotNull(outerFrame);
    	this.tbl = XkeyOp.INSTANCE.unkey(m);
    }
    
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
