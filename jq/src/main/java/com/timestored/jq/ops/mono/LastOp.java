package com.timestored.jq.ops.mono;

import com.timestored.jq.ops.IndexOp;

public class LastOp extends BaseMonad {
	public static LastOp INSTANCE = new LastOp();
	@Override public String name() {return "last"; }

	@Override public Object run(Object a) {
		if(TypeOp.isNotList(a)) {
			return a;
		}
		return IndexOp.INSTANCE.run(a, CountOp.INSTANCE.count(a)-1);	
	}

}
