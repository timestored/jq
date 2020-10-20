package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.LongCol;
import com.timestored.jq.RankException;
import com.timestored.jq.ops.CastOp;
public class IAscOp extends BaseMonad {
	public static IAscOp INSTANCE = new IAscOp();
	@Override public String name() { return "iasc"; }

    public LongCol run(Object a) {
		if(a instanceof Col) {
			return CastOp.CAST.j(((Col) a).iasc());
		}
		throw new RankException();
    }

}

