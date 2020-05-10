package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;

public class DescOp extends BaseMonad {
	public static DescOp INSTANCE = new DescOp();
	@Override public String name() { return "desc"; }

	@Override public Object run(Object a) {
		if(a instanceof Col) {
			return ReverseOp.INSTANCE.run(AscOp.INSTANCE.run(a));
		}
		return a;
	}
}
