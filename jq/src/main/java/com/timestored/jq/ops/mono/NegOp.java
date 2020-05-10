package com.timestored.jq.ops.mono;

import com.timestored.jq.ops.MulOp;

public class NegOp extends BaseMonad {
	public static NegOp INSTANCE = new NegOp();
	@Override public String name() { return "neg"; }

    public Object run(Object o) {
    	return MulOp.INSTANCE.run(-1, o);
    }
}
