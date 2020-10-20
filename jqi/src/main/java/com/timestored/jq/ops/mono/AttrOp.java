package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;

public class AttrOp extends BaseMonad {
	public static AttrOp INSTANCE = new AttrOp();
	@Override public String name() { return "attr"; }

	@Override public Object run(Object a) {
		return (a instanceof Col && ((Col)a).isSorted()) ? "s" : "";
	}

}
