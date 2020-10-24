package com.timestored.jq.ops; 
public class MatchOp extends BaseDiad {
	public static MatchOp INSTANCE = new MatchOp(); 
	@Override public String name() { return "~"; }

	@Override public Object run(Object a, Object b) {
		if(a == null) {
			return b == null;
		}
		return a==b || a.equals(b); 
	}
  	
}