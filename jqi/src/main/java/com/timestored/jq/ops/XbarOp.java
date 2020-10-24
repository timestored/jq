package com.timestored.jq.ops; 

public class XbarOp extends BaseDiad {
	public static XbarOp INSTANCE = new XbarOp();
	@Override public String name() { return "xbar"; }
	@Override public Object run(Object x, Object y) {
        return MulOp.INSTANCE.run(x, DivOp.INSTANCE.run(y,x));
	}
}