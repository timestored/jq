package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.iterator.RangeLocations;
import com.timestored.jq.RankException;

public class PrevOp extends BaseMonad {
	public static PrevOp INSTANCE = new PrevOp();
	@Override public String name() { return "prev"; }

	@Override public Object run(Object a) {
		if(a instanceof Col) {
			Col c = (Col) a;
			return c.select(new RangeLocations(-1, ((Col) a).size()));
		}
		throw new RankException("Can't do anything to non cols in next/prev");
	}

}
