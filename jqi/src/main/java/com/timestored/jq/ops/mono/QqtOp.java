package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.Tbl;

public class QqtOp extends BaseMonad {
	public static QqtOp INSTANCE = new QqtOp(); 
	@Override public String name() { return ".Q.qt"; }
	@Override public Object run(Object a) { return ex(a); }; 
	
	public boolean ex(Object a) {
		if(a instanceof Tbl) {
			return true;
		} if(a instanceof Mapp) {
			Mapp m = (Mapp)a;
			return m.getKey() instanceof Tbl && m.getValue() instanceof Tbl;
		}
		return false;
	}
}
