package com.timestored.jq.ops;

public class GreaterThanOp extends BaseDiad {
	public static GreaterThanOp INSTANCE = new GreaterThanOp(); 
	@Override public String name() { return ">"; }	
	@Override public Object run(Object a, Object b) { return LessThanOp.INSTANCE.run(b,a); }
}