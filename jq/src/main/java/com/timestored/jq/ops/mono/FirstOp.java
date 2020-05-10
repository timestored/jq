package com.timestored.jq.ops.mono;

import com.timestored.jq.ops.IndexOp;

public class FirstOp extends BaseMonad {
	public static FirstOp INSTANCE = new FirstOp();
	@Override public String name() {return "first"; }

	@Override public Object run(Object a) {
		if(TypeOp.isNotList(a)) {
			return a;
		}
		return IndexOp.INSTANCE.run(a, 0);	
	}

}
