package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jq.TypeException;

public class KeysOp extends BaseMonad {
	public static KeysOp INSTANCE = new KeysOp();
	@Override public String name() { return "keys"; }

	@Override public Object run(Object a) {return ex(a);	}

	public StringCol ex(Object a) {
		if(a instanceof Tbl) {
			return ColProvider.emptyStringCol;
		} else if(a instanceof Mapp && QqtOp.INSTANCE.ex(a)) {
			return ((Tbl) ((Mapp) a).getKey()).getKey();
		}
		throw new TypeException("keys only works on tables.");
	}
}
