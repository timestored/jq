package com.timestored.jq.ops.mono;

import com.timestored.jq.ops.SubOp;

public class IDescOp extends BaseMonad {
	public static IDescOp INSTANCE = new IDescOp();
	@Override public String name() { return "idesc"; }

    public Object run(Object o) {
    	// To make it stable for descending
    	// {reverse (count[x]-1)-iasc reverse x}
    	Object revIndices = IAscOp.INSTANCE.run(ReverseOp.INSTANCE.run(o));
    	return ReverseOp.INSTANCE.run(SubOp.INSTANCE.run(CountOp.INSTANCE.count(o)-1, revIndices));
    }
}
