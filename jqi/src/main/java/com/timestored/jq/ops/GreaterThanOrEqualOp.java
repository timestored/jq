package com.timestored.jq.ops;

public class GreaterThanOrEqualOp extends BaseDiad {
	public static GreaterThanOrEqualOp INSTANCE = new GreaterThanOrEqualOp(); 
	@Override public String name() { return ">="; }	
	@Override public Object run(Object a, Object b) { return LessThanOrEqualOp.INSTANCE.run(b,a); }
}