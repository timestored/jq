package com.timestored.jq.ops;

import com.timestored.jq.TypeException;

public class AssignOp extends BaseDiad {
	public static AssignOp INSTANCE = new AssignOp(); 
	@Override public String name() { return ":"; }

	@Override public Object run(Object a, Object b) {
		if(a instanceof String) {
			this.frame.assign((String) a, b);
			return b;
		}
		throw new TypeException("Can only assign strings, not:" + a.toString());
	}
  	
}