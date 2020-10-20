package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.iterator.RangeLocations;
import com.timestored.jq.RankException;

public class NextOp extends BaseMonad {
	public static NextOp INSTANCE = new NextOp();
	@Override public String name() { return "next"; }

	@Override public Object run(Object a) {
		if(a instanceof Col) {
			Col c = (Col) a;
			return c.select(new RangeLocations(1, ((Col) a).size()+1));
		}
		throw new RankException("Can't do anything to non cols in next/prev");
	}

}
