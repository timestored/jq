package com.timestored.jq.ops.mono;

public class NiladicOp extends BaseMonad {
	public static NiladicOp INSTANCE = new NiladicOp();

	@Override public Object run(Object a) { return a;	}

	@Override public String name() {return "::"; }
	@Override public String toString() { return name(); }
}
