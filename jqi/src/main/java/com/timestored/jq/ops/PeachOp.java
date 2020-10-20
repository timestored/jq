package com.timestored.jq.ops;

public class PeachOp extends BaseDiad {
	public static PeachOp INSTANCE = new PeachOp(); 
	@Override public String name() { return "peach"; }

	@Override public Object run(Object a, Object b) {
		EachOp.INSTANCE.setContext(context);
		EachOp.INSTANCE.setFrame(frame);
		return EachOp.INSTANCE.run(a, b);
	}
  	
}