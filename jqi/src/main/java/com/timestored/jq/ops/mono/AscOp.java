package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jq.RankException;

public class AscOp extends BaseMonad {
	public static AscOp INSTANCE = new AscOp();
	@Override public String name() { return "asc"; }

	@Override public Object run(Object a) {
		if(a instanceof Col) {
			return ((Col) a).sort();
		}
		throw new RankException();
	}

}
